



// Configuration constants for the ETL process //

// Test Folder
// pub const INPUT_PREFIX : &str = "xmlreader/dummypnr/"; 
// pub const FOLDER_NAME : &str = "gluejob";



pub const TIME_STAMP : &str = "20281115";

pub const FOLDER_NAME : &str = "saas_etl_raw_files/Datafiles/RawProcessedCSVFromXMLTicketData"; 
pub const INPUT_BUCKET : &str = "anxi-saas-uat-test";
pub const OUTPUT_BUCKET : &str = "anxi-saas-uat-test"; 
pub const CSV_PREFIX : &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE : usize = 100000usize;
pub const _TIME_FORMAT : &str = "%Y%m%d";
pub const EXTENSION : &str = ".csv";
// pub const INPUT_PREFIX : &str = "xmlreader/20251115/"; 

pub fn inputprefix() -> String {
    format!("saas_etl_raw_files/Datafiles/RawXMLTicketDataFromClient/{}/", TIME_STAMP)
}