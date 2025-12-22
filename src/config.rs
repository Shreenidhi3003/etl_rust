



// Configuration constants for the ETL process //

pub const INPUT_PREFIX : &str = "xmlreader/"; 
pub const INPUT_BUCKET : &str = "anxi-temp-testfiles";
pub const OUTPUT_BUCKET : &str = "anxi-temp-testfiles"; 
pub const CSV_PREFIX : &str = "output_csv_file";
pub const MAX_ROWS_PER_FILE : usize = 10000usize;
pub const TIME_FORMAT : &str = "%Y%m%d";