use anyhow::Result;
use aws_sdk_lambda::Client as LambdaClient;
use serde::Serialize;
use serde_json::json;
use chrono::{Local, DateTime};


const PIPELINE_NAME: &str = "MH_TICKETING_PIPELINE";
const JOB_NAME: &str = "TicketingXMLToCSV";
const JOB_TYPE: &str = "BatchTest";
const LOGGER_LAMBDA_NAME: &str = "MH_SAAS_PROD_DailyJobsLogTriggering";

#[derive(Serialize)]
struct PostgresLog {
    execution_id:String,
    pipeline_name:String,
    job_name:String,
    job_type:String,
    status:String,
    remarks:String,
    fileloadeddate:Option<DateTime<Local>>

}

 pub async fn call_logger_lambda(
    lambda_client:&LambdaClient,
    execution_id:String,
    status:String,
    remarks:String,
    send_email:bool
) -> Result<(),Box<dyn std::error::Error>> {
    let postgres_log = PostgresLog {
        execution_id,
        pipeline_name:PIPELINE_NAME.to_string(),
        job_name:JOB_NAME.to_string(),
        job_type:JOB_TYPE.to_string(),
        status,
        remarks,
        fileloadeddate:Some(Local::now())
    };

    let mut payload = json!({
        "postgres_log":postgres_log
    });

    if send_email {
        payload["email_log"] = payload["postgres_log"].clone();
    }

    let response = lambda_client.invoke()
        .function_name(LOGGER_LAMBDA_NAME)
        .payload(payload.to_string().into_bytes().into())
        .send()
        .await?;

    if let Some(bytes) = response.payload {
        let response_text = String::from_utf8(bytes.into_inner());
        println!("Response from lambda: {:?}",response_text);
    }
    

    Ok(())
}

