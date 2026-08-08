// Configuration constants for the ETL process //

// Test Folder
// pub const INPUT_PREFIX: &str = "xmlreader/dummypnr/";
// pub const FOLDER_NAME: &str = "gluejob";

pub const TIME_STAMP: &str = "20260807";

pub const TIME_STAMPS: &[&str] = &[
    TIME_STAMP,
];

// "20281116", "20281117", "20281118", "20281119", "20281120", "20281121",

pub const FOLDER_NAME: &str = "RawData/anxilla-1a-rawdata/TicketingXMLDataProcessed";
pub const INPUT_BUCKET: &str = "mh-saas-arms-prod"; // mh-saas-arms-uat
pub const OUTPUT_BUCKET: &str = "mh-saas-arms-prod"; // mh-saas-arms-uat
pub const CSV_PREFIX: &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE: usize = 100000usize;
pub const _TIME_FORMAT: &str = "%Y%m%d";
pub const EXTENSION: &str = ".csv";

// pub const INPUT_PREFIX: &str = "xmlreader/20251115/";

// RawXMLTicketDataFromClient
pub fn inputprefix(timestamp: &str) -> String {
    format!(
        "RawData/anxilla-1a-rawdata/TicketingXMLData/{}/",
        timestamp
    )
}