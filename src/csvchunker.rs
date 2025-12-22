use crate::models::Record;
use anyhow::{Ok, Result};
use csv::Writer;
use std::fs::File;
use std::path::Path;
use aws_sdk_s3::Client;
use tokio::fs;


pub struct CsvChunkerWriter {
    prefix: String,
    file_index: usize,
    current_rows: usize,
    max_rows: usize,
    bucket: String,
    client: Client,
    writer: Writer<File>,
    timestamp: String,
}

impl CsvChunkerWriter {
    pub async fn new(prefix: &str, bucket: &str, max_rows: usize, client: Client, timestamp: &str) -> Result<Self> {
        let file_index = 1usize;

        std::fs::create_dir_all(prefix)?;
        let filename = format!("{}_{}.csv", prefix, file_index);
        let writer = Writer::from_path(&filename)?;

        Ok(Self {
            prefix: prefix.to_string(),
            file_index,
            current_rows: 0,
            max_rows,
            bucket: bucket.to_string(),
            client,
            writer,
            timestamp: timestamp.to_string(),
        })
    }

    fn current_name(&self) -> String {
        format!("{}_{}.csv", self.prefix, self.file_index)
    }

    async fn rotate(&mut self) -> Result<()> {
        // flush csv writer to ensure content is on disk
        self.writer.flush()?;

        let filename = self.current_name();
        let key = format!("gluejob/{}/{}", self.timestamp, filename);

        // read the file contents (async)
        let data = fs::read(&filename).await?;

        // upload bytes to S3 (don't shadow `data` variable)
        crate::aws::upload_s3_bytes(&self.client, &key, &self.bucket, data).await?;

        // remove the local file
        if Path::new(&filename).exists() {
            fs::remove_file(&filename).await?;
        }

        // rotate index and create new writer
        self.file_index += 1;
        self.current_rows = 0;
        let new_filename = self.current_name();
        self.writer = Writer::from_path(&new_filename)?;

        Ok(())
    }

    pub async fn write_record(&mut self, rec: &Record) -> Result<()> {
        // rotate before writing the next row if reached limit
        if self.current_rows >= self.max_rows {
            self.rotate().await?;
        }
        self.writer.serialize(rec)?;
        self.current_rows += 1;
        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<()> {
        self.writer.flush()?;

        let filename = self.current_name();
        let key = format!("gluejob/{}/{}", self.timestamp, filename);
        let data = fs::read(&filename).await?;

        if Path::new(&filename).exists() {
            fs::remove_file(&filename).await?;
        }

        crate::aws::upload_s3_bytes(&self.client, &key, &self.bucket, data).await?;

        Ok(())


    }
}
