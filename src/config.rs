// Configuration constants for the ETL process //

// Test Folder
// pub const INPUT_PREFIX: &str = "xmlreader/dummypnr/";
// pub const FOLDER_NAME: &str = "gluejob";

pub const TIME_STAMP: &str = "20260429";

pub const TIME_STAMPS: &[&str] = &[
    TIME_STAMP,
    "20260430",
    "20260501",
    "20260502",
    "20260503",
    "20260504",
    "20260505",
    "20260506",
    "20260507",
    "20260508",
    "20260509",
    "20260510",
    "20260511",
    "20260512",
    "20260513",
    "20260514",
    "20260515",
    "20260516",
    "20260517",
    "20260518",
    "20260519",
    "20260520",
    "20260521",
    "20260522",
    "20260523",
    "20260524",
    "20260525",
    "20260526",
    "20260527",
    "20260528",
    "20260529",
    "20260530",
];

// "20281116", "20281117", "20281118", "20281119", "20281120", "20281121",

pub const FOLDER_NAME: &str = "TicketingRawDataProcessed";
pub const INPUT_BUCKET: &str = "mh-saas-arms-uat"; // anxi-saas-uat-test
pub const OUTPUT_BUCKET: &str = "mh-saas-arms-uat";
pub const CSV_PREFIX: &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE: usize = 100000usize;
pub const _TIME_FORMAT: &str = "%Y%m%d";
pub const EXTENSION: &str = ".csv";

// pub const INPUT_PREFIX: &str = "xmlreader/20251115/";

// RawXMLTicketDataFromClient
pub fn inputprefix(timestamp: &str) -> String {
    format!(
        "TicketingRawData/{}/",
        timestamp
    )
}