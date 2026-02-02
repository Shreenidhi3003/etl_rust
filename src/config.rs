



// Configuration constants for the ETL process //

// Test Folder
// pub const INPUT_PREFIX : &str = "xmlreader/dummypnr/"; 
// pub const FOLDER_NAME : &str = "gluejob";


// UAT Folder
pub const INPUT_PREFIX : &str = "xmlreader/20251115/"; 
pub const FOLDER_NAME : &str = "rustrawdata/tblrawticketing"; 

pub const INPUT_BUCKET : &str = "anxi-temp-testfiles";
pub const OUTPUT_BUCKET : &str = "anxi-saas-uat-test";
pub const CSV_PREFIX : &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE : usize = 100000usize;
pub const TIME_FORMAT : &str = "%Y%m%d";
pub const EXTENSION : &str = ".csv";