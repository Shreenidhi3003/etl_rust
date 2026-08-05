// Configuration constants for the ETL process //

// Test Folder
// pub const INPUT_PREFIX: &str = "xmlreader/dummypnr/";
// pub const FOLDER_NAME: &str = "gluejob";

pub const TIME_STAMP: &str = "20260502";

pub const TIME_STAMPS: &[&str] = &[
    TIME_STAMP,
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
    "20260531",
    "20260601",
    "20260602",
    "20260603",
    "20260604",
    "20260605",
    "20260606",
    "20260607",
    "20260608",
    "20260609",
    "20260610",
    "20260611",
    "20260612",
    "20260613",
    "20260614",
    "20260615",
    "20260616",
    "20260617",
    "20260618",
    "20260619",
    "20260620",
    "20260621",
    "20260622",
    "20260623",
    "20260624",
    "20260625",
    "20260626",
    "20260627",
    "20260628",
    "20260629",
    "20260630",
    "20260701",
    "20260702",
    "20260703",
    "20260704",
    "20260705",
    "20260706",
    "20260707",
    "20260708",
    "20260709",
    "20260710",
    "20260711",
    "20260712",
    "20260713",
    "20260714",
    "20260715",
    "20260716",
    "20260717",
    "20260718",
    "20260719",
    "20260720",
    "20260721",
    "20260722",
    "20260723",
    "20260724",
    "20260725",
    "20260726",
    "20260727",
    "20260728",
    "20260729",
    "20260730",
    "20260731",
    "20260801",
    "20260802",
    "20260803",
    "20260804",
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