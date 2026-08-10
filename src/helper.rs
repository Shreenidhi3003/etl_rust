use anyhow::Result;
use chrono::{NaiveDate, Duration, Local};
use sqlx::Row;

pub async fn load_dates_list() -> Result<()> {

    let pool = crate::db::create_db_connection().await?;

    let data = sqlx::query(
        r#"
        SELECT max(fileloadeddate)
        FROM mh_arms_aws_prod.sys_dailyjobeventlog
        WHERE pipelinename = 'MH_TICKETING_PIPELINE'
          AND jobname = 'TicketingXMLToCSV'
        "#
    )
    .fetch_one(&pool)
    .await?;

    println!("Data: {:?}", data);

    let max_load_date:Option<String> = data.get(0);
    let max_load_date = match max_load_date {
        Some(date) => date,
        None => {
            println!("No previous load date found.");
            return Ok(());
        }
    };
    // Today's date
    let today = Local::now().date_naive();

    // Convert database date to NaiveDate
    let mut current =
        NaiveDate::parse_from_str(&max_load_date, "%Y%m%d")
            .unwrap()
            + Duration::days(1);

    let mut date_list: Vec<String> = Vec::new();

    while current <= today {
        date_list.push(current.format("%Y%m%d").to_string());
        current += Duration::days(1);
    }

    println!("{:?}", date_list);

    Ok(())
}