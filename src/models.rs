use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord};
use indicatif::ProgressBar;
use std::fs::File;
use std::io::{BufReader, Write};
use std::sync::{Arc, Mutex};
use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenv::dotenv;

#[allow(dead_code)]
#[derive(sqlx::FromRow)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
}

#[allow(dead_code)]
pub async fn create_user(pool: &sqlx::PgPool, name: &str, email: &str) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO users (name, email) VALUES ($1, $2)")
        .bind(name)
        .bind(email)
        .execute(pool)
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn get_user(pool: &sqlx::PgPool, user_id: i32) -> Result<User, sqlx::Error> {
    let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(pool)
        .await?;
    Ok(user)
}

#[allow(dead_code)]
pub async fn update_user_email(pool: &sqlx::PgPool, user_id: i32, new_email: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE users SET email = $1 WHERE id = $2")
        .bind(new_email)
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn delete_user(pool: &sqlx::PgPool, user_id: i32) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub fn get_csv_headers(csv_path: &str, delimiter: u8) -> Result<Vec<String>> {
    let file = File::open(csv_path).context("Failed to open CSV file")?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
        .from_reader(BufReader::new(file));
    
    let headers = rdr
        .headers()
        .context("Failed to read CSV headers")?
        .iter()
        .map(|h| {
            let cleaned = h
                .chars()
                .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
                .collect::<String>();
            let mut result = cleaned.to_lowercase();
            if result.chars().next().map_or(false, |c| c.is_numeric()) {
                result = format!("col_{}", result);
            }
            result
        })
        .collect();
    
    Ok(headers)
}

pub async fn create_table(pool: &sqlx::PgPool, headers: &[String], table_name: &str) -> Result<()> {
    let query_drop = format!(
        "DROP TABLE IF EXISTS {}",
        table_name
    );

    sqlx::query(&query_drop)
        .execute(pool)
        .await?;

    let columns = headers
        .iter()
        .map(|h| format!("{} TEXT", h))
        .collect::<Vec<_>>()
        .join(", ");

    
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table_name, columns
    );

    sqlx::query(&query)
        .execute(pool)
        .await?;

    if headers.contains(&String::from("origin")){
        let query = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_origin ON {} (origin)",
            table_name, table_name
        );
        sqlx::query(&query)
            .execute(pool)
            .await?;
    }
    if headers.contains(&String::from("path")){
        let query = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_path ON {} (path)",
            table_name, table_name
        );
        sqlx::query(&query)
            .execute(pool)
            .await?;
    }
    if headers.contains(&String::from("branch")){
        let query = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_branch ON {} (branch)",
            table_name, table_name
        );
        sqlx::query(&query)
            .execute(pool)
            .await?;
    }
    if headers.contains(&String::from("normalized_branch")){
        let query = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_normalized_branch ON {} (normalized_branch)",
            table_name, table_name
        );
        sqlx::query(&query)
            .execute(pool)
            .await?;
    }

    println!("Table created or verified: {}", table_name);
    
    Ok(())
}

pub async fn process_csv_file(pool: &sqlx::PgPool, headers: &[String], delimiter: u8, file_path_str: &str, table_name: &str) -> Result<()>{
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
        .from_path(file_path_str)?;
    let len_error = Arc::new(Mutex::new(vec![]));
    for record in reader.records(){
        match record{
            Err(e) => {
                eprintln!("Error reading record {}", e);
            }
            Ok(s) => {
                insert_path(pool, headers, s, len_error.clone(), table_name).await?;
            }
        }
    }

    if let Ok(errors) = len_error.lock() {
        if !errors.is_empty() {
            let error_file_path = format!("{}.errors.txt", file_path_str);
            let mut file = std::fs::File::create(error_file_path)?;
            
            for error_record in errors.iter() {
                let line = error_record.join(",");
                writeln!(file, "{}", line)?;
            }
            
            println!("Wrote {} error records to {}.errors.txt", errors.len(), file_path_str);
        }
        else{
            println!("Successfully processed all records with no errors");
            let error_file_path = format!("{}.errors.txt", file_path_str);
            if std::path::Path::new(&error_file_path).exists() {
                std::fs::remove_file(&error_file_path)?;
                println!("Removed old error file: {}", error_file_path);
            }
        }
    }
    Ok(())
}

pub async fn insert_path(pool: &sqlx::PgPool, headers: &[String], record: StringRecord, errors: Arc<Mutex<Vec<Vec<String>>>>, table_name: &str) -> Result<(), sqlx::Error> {
    let columns = headers
        .iter()
        .map(|h| String::from(h))
        .collect::<Vec<_>>()
        .join(", ");

    let values: Vec<String> = record.iter()
        .map(|s| s.to_string())
        .collect();

    if values.len() != headers.len(){
        println!("values.len() == {} | values: {:?} | record: {:?}",values.len(), values, record);
        let values_bis: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        println!("values_bis: {:?}", values_bis);
        if let Ok(mut errors_guard) = errors.lock() {
            errors_guard.push(values);
        }
        return Ok(());
    }

    let placeholders = (1..=values.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");

    let query_str = format!("INSERT INTO {} ({}) VALUES ({})", table_name, columns, placeholders);

    let mut query = sqlx::query(&query_str);
    for value in &values{
        query = query.bind(value);
    }
    query.execute(pool)
        .await?;

    Ok(())
}

pub async fn convert_modified_files(file_path: &str) -> Result<()>{
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to create pool.")?;

    println!("Connected to the database!");

    let table_name = "modified_files";
    let headers = get_csv_headers(file_path, b',')?;
    println!("{}: {:?}", table_name, headers);
    create_table(&pool, &headers, table_name).await?;
    process_csv_file(&pool, &headers, b',', file_path, table_name).await?;
    Ok(())
}

pub async fn convert_altered_histories(directory_path: &str) -> Result<()>{
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to create pool.")?;

    println!("Connected to the database!");

    let entries = std::fs::read_dir(directory_path)
        .context(format!("Failed to read directory: {}", directory_path))?;

    let csv_files: Vec<String> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("csv") {
                path.to_str().map(|s| s.to_owned())
            } else {
                None
            }
        })
        .collect();

    let headers = get_csv_headers(csv_files.get(0).expect("No csv files"), b';')?;
    let table_name = "altered_histories";
    create_table(&pool, &headers, table_name).await?;
    println!("{}: {:?}", table_name, headers);
    let bar = Arc::new(ProgressBar::new(csv_files.len() as u64));
    for file in csv_files{
        let bar = bar.clone();
        bar.inc(1);
        process_csv_file(&pool, &headers, b';', &file, table_name).await?;
    }

    Ok(())
}

pub async fn convert_altered_histories_single_file(file_path: &str) -> Result<()>{
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to create pool.")?;

    println!("Connected to the database!");


    let headers = get_csv_headers(file_path, b',')?;
    let table_name = "altered_histories_clean";
    create_table(&pool, &headers, table_name).await?;
    println!("{}: {:?}", table_name, headers);

    process_csv_file(&pool, &headers, b',', file_path, table_name).await?;

    Ok(())
}
