use anyhow::Result;
use chrono::{Duration, Local, DateTime, Utc};
use sqlx::Row;

pub async fn load_dates_list() -> Result<Vec<String>> {

    let pool = crate::db::create_db_connection().await?;

    let data = sqlx::query(
        r#"
        SELECT max(fileloadeddate) as max_load_date
        FROM mh_arms_aws_prod.sys_dailyjobeventlog
        WHERE pipelinename = 'MH_TICKETING_PIPELINE'
          AND jobname = 'TicketingXMLToCSV'
          AND jobtype = 'Batch'
        "#
    )
    .fetch_one(&pool)
    .await?;

    println!("Data: {:?}", data);
    
    let today = Local::now().date_naive();
    let max_load_date:Option<DateTime<Utc>> = data.try_get("max_load_date")?;
    let max_load_date = match max_load_date {
        Some(date) => date.date_naive(),
        None => {
            println!("No previous load date found.");
            return Ok(vec![
                today.format("%Y%m%d").to_string()
            ]);
        }
    };
    // Today's date

    // Convert database date to NaiveDate
    let mut current = max_load_date + Duration::days(1);
    let mut date_list: Vec<String> = Vec::new();

    while current <= today {
        date_list.push(current.format("%Y%m%d").to_string());
        current += Duration::days(1);
    }

    println!("{:?}", date_list);

    Ok(date_list)
}