mod aws;
mod config;
mod csvchunker;
mod models;
mod parser;
mod logs;
mod db;
mod helper;

use anyhow::Result;
use aws_sdk_s3::Client;
use aws_sdk_lambda::Client as LambdaClient;
use quick_xml::Reader;
use std::io::Cursor;
use std::println;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {

    let lambda_client: LambdaClient = crate::aws::make_lambda_client().await;
    let execution_id = std::env::var("AWS_BATCH_JOB_ID").unwrap_or_else(|_| "UNKNOWN".to_string());
    match run_job(&lambda_client, execution_id.clone()).await {
        Ok(loaded) => {
            let _success = crate::logs::call_logger_lambda(&lambda_client,execution_id,"success".to_string(),"XML Processing Completed".to_string(),false).await;
            println!("XML Processing Completed Successfully{:?}",loaded);
        }
        Err(e) => {
            let _failure = crate::logs::call_logger_lambda(&lambda_client,execution_id,"failed".to_string(),"XML Processing Failed".to_string(),false).await;
            println!("XML Processing Failed: {:?}", e);
        }
    }
    Ok(())
}

async fn run_job(lambda_client:&LambdaClient,execution_id: String) -> Result<()> {
    let start_time = Instant::now();
    let input_bucket: &str = config::INPUT_BUCKET;
    let output_bucket: &str = config::OUTPUT_BUCKET;
    let csv_prefix: &str = config::CSV_PREFIX;
    let max_rows_per_chunk = config::MAX_ROWS_PER_FILE;

    let timestamps_list = crate::helper::load_dates_list().await?;
    println!("timestamps_list: {:?}", timestamps_list);
    
    let _initiated = crate::logs::call_logger_lambda(&lambda_client,execution_id,"initiated".to_string(),"XML Processing Started".to_string(),false).await;
    
    // init client
    let client: Client = crate::aws::make_s3_client().await;
    // let lambda_client: LambdaClient = crate::aws::make_lambda_client().await;

    // let execution_id = String::from("");


    // config::TIME_STAMPS
    for timestamp in timestamps_list {
        println!("Processing timestamp {}", timestamp);
        let input_prefix: String = config::inputprefix(&timestamp);

        // list keys (propagate errors)
        let list_of_keys =
            crate::aws::list_of_xml_from_s3(&client, input_bucket, &input_prefix).await?;

        // create csv chunker (clone client because CsvChunker takes an owned Client)
        let mut csv_writer = crate::csvchunker::CsvChunkerWriter::new(
            csv_prefix,
            output_bucket,
            max_rows_per_chunk,
            client.clone(),
            &timestamp,
        )
        .await?;

        for key in list_of_keys {
            println!("Processing {:?}", key);

            // get object body as ByteStream and collect bytes
            let body_stream = crate::aws::get_object_body(&client, &key, input_bucket).await?;
            let collected = body_stream.collect().await?;
            let bytes = collected.into_bytes().to_vec();

            // build a Reader from the downloaded bytes
            let mut xml_reader = Reader::from_reader(Cursor::new(bytes));
            xml_reader.trim_text(true);

            // parse XML into Vec<Record>
            let records = crate::parser::parse_xml(&mut xml_reader)?;
            println!("Parsed {} records", records.len());

            // write entries into CSV chunker
            for rec in records {
                csv_writer.write_record(&rec).await?;
            }
        }

        csv_writer.finalize().await?;
    }
    let duration = start_time.elapsed();
    println!("Processing completed in: {:?}", duration);

    Ok(())
}
 