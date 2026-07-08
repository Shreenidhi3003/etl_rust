// Configuration constants for the ETL process //

// Test Folder
// pub const INPUT_PREFIX : &str = "xmlreader/dummypnr/";
// pub const FOLDER_NAME : &str = "gluejob";

pub const TIME_STAMP: &str = "20251116";
pub const TIME_STAMPS: &[&str] = &[
    TIME_STAMP, "20251117", "20251118", "20251119", "20251120", "20251121",
];
//  "20281116", "20281117", "20281118", "20281119", "20281120", "20281121",
pub const FOLDER_NAME: &str = "saas_etl_raw_files/Datafiles/RawProcessedCSVFromXMLTicketDataDummy";
pub const INPUT_BUCKET: &str = "anxi-saas-uat-test";
pub const OUTPUT_BUCKET: &str = "anxi-saas-uat-test";
pub const CSV_PREFIX: &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE: usize = 100000usize;
pub const _TIME_FORMAT: &str = "%Y%m%d";
pub const EXTENSION: &str = ".csv";
// pub const INPUT_PREFIX : &str = "xmlreader/20251115/";

pub fn inputprefix(timestamp: &str) -> String {
    format!(
        "saas_etl_raw_files/Datafiles/RawXMLTicketDataFromClient/{}/",
        timestamp
    )
}
