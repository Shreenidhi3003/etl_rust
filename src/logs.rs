use anyhow::Result;
use aws_sdk_lambda::Client as LambdaClient;
use serde::Serialize;
use serde_json::json;
use chrono::{NaiveDate, Utc};
use chrono_tz::Asia::Kuala_Lumpur;

#[derive(Serialize)]
struct PostgresLog {
    execution_id:String,
    pipeline_name:String,
    job_name:String,
    job_type:String,
    status:String,
    remarks:String,
    fileloadeddate:Option<NaiveDate>

}

 pub async fn call_logger_lambda(
    lambda_client:&LambdaClient,
    execution_id:String,
    status:String,
    remarks:String,
    send_email:bool
) -> Result<(),Box<dyn std::error::Error>> {
    let malaysia_now = Utc::now().with_timezone(&Kuala_Lumpur);
    let postgres_log = PostgresLog {
        execution_id,
        pipeline_name:crate::config::PIPELINE_NAME.to_string(),
        job_name:crate::config::JOB_NAME.to_string(),
        job_type:crate::config::JOB_TYPE.to_string(),
        status,
        remarks,
        fileloadeddate:Some(malaysia_now.date_naive())
    };

    let mut payload = json!({
        "postgres_log":postgres_log
    });

    if send_email {
        payload["email_log"] = payload["postgres_log"].clone();
    }

    let response = lambda_client.invoke()
        .function_name(crate::config::LOGGER_LAMBDA_NAME)
        .payload(payload.to_string().into_bytes().into())
        .send()
        .await?;

    if let Some(bytes) = response.payload {
        let response_text = String::from_utf8(bytes.into_inner());
        println!("Response from lambda: {:?}",response_text);
    }
    

    Ok(())
}

