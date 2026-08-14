// Configuration constants for the ETL process //

pub const PIPELINE_NAME: &str = "MH_TICKETING_PIPELINE";
pub const JOB_NAME: &str = "TicketingXMLToCSV";
pub const JOB_TYPE: &str = "Batch";
pub const LOGGER_LAMBDA_NAME: &str = "MH_SAAS_PROD_DailyJobsLogTriggering";
pub const SECRET_NAME: &str = "rds!cluster-5d580938-6ed5-454d-bfff-e404563b1911";

pub const FOLDER_NAME: &str = "RawData/anxilla-1a-rawdata/TicketingXMLDataProcessed";
pub const INPUT_BUCKET: &str = "mh-saas-arms-prod"; 
pub const OUTPUT_BUCKET: &str = "mh-saas-arms-prod"; 
pub const CSV_PREFIX: &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE: usize = 100000usize;
pub const _TIME_FORMAT: &str = "%Y%m%d";
pub const EXTENSION: &str = ".csv";

pub fn inputprefix(timestamp: &str) -> String {
    format!(
        "RawData/anxilla-1a-rawdata/TicketingXMLData/{}/",
        timestamp
    )
}