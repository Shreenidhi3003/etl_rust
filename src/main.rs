mod aws;
mod parser;
mod models;
mod csvchunker;
mod config;

use anyhow::Result;
use aws_sdk_s3::Client;
use quick_xml::Reader;
use std::io::Cursor;
use chrono::Local;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {

    let start_time = Instant::now();
    let timestamp = Local::now().format(config::TIME_FORMAT).to_string();
    let input_prefix: &str = config::INPUT_PREFIX; 
    let input_bucket: &str = config::INPUT_BUCKET; 
    let output_bucket: &str = config::OUTPUT_BUCKET;
    let csv_prefix: &str = config::CSV_PREFIX;
    let max_rows_per_chunk = config::MAX_ROWS_PER_FILE; 

    // init client
    let client: Client = crate::aws::make_s3_client().await;

    // list keys (propagate errors)
    let list_of_keys = crate::aws::list_of_xml_from_s3(&client, input_bucket, input_prefix).await?;

    // create csv chunker (clone client because CsvChunker takes an owned Client)
    let mut csv_writer = crate::csvchunker::CsvChunkerWriter::new(
        csv_prefix,
        output_bucket,
        max_rows_per_chunk,
        client.clone(),
        timestamp.as_str(),
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
    let duration = start_time.elapsed();
    println!("Processing completed in: {:?}", duration);
     

    Ok(())
}
