use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, Write};
use std::sync::{Arc, Mutex};
use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenv::dotenv;
use futures::future::try_join_all;
use std::time::{Duration, Instant};

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
    
    // Get file size and setup progress tracking
    let file_size = get_file_size(file_path_str);
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);
    
    // Estimate total records (rough estimate: file_size / average_record_size)
    // For most CSV files, average record size is around 100-500 bytes
    let estimated_records = (file_size / 200) as u64; // Conservative estimate
    
    // Setup progress bar
    let progress_bar = ProgressBar::new(estimated_records);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} records ({percent}%) | {per_sec} | ETA: {eta} | Memory: {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );
    progress_bar.set_message(get_memory_usage());
    
    println!("Processing file: {} ({:.2} MB)", file_path_str, file_size_mb);
    println!("Estimated records: {}", estimated_records);
    
    // Process records in streaming batches to avoid memory issues
    let (batch_size, max_concurrent_batches) = calculate_batch_params(file_size_mb);
    
    let mut current_batch = Vec::new();
    let mut active_futures = Vec::new();
    let mut total_processed = 0u64;
    let start_time = Instant::now();
    let mut last_memory_check = Instant::now();
    
    println!("Starting streaming CSV processing with batch size {} and max {} concurrent batches...", batch_size, max_concurrent_batches);
    
    for record_result in reader.records() {
        match record_result {
            Err(e) => {
                eprintln!("Error reading record: {}", e);
                continue;
            }
            Ok(record) => {
                current_batch.push(record);
                
                // When batch is full, spawn a task to process it
                if current_batch.len() >= batch_size {
                    let pool = pool.clone();
                    let headers = headers.to_vec();
                    let table_name = table_name.to_string();
                    let len_error = len_error.clone();
                    let batch_data = std::mem::take(&mut current_batch);
                    
                    let future = tokio::spawn(async move {
                        if let Err(e) = insert_batch(&pool, &headers, batch_data, len_error.clone(), &table_name).await {
                            eprintln!("Error inserting batch: {}", e);
                        }
                    });
                    active_futures.push(future);
                    total_processed += batch_size as u64;
                    
                    // Update progress bar
                    progress_bar.set_position(total_processed);
                    
                    // Update memory usage every 5 seconds
                    if last_memory_check.elapsed() > Duration::from_secs(5) {
                        progress_bar.set_message(get_memory_usage());
                        last_memory_check = Instant::now();
                    }
                    
                    // Limit concurrent tasks to prevent overwhelming the system
                    if active_futures.len() >= max_concurrent_batches {
                        try_join_all(active_futures.drain(..)).await?;
                    }
                }
            }
        }
    }
    
    // Process any remaining records in the last batch
    if !current_batch.is_empty() {
        let pool = pool.clone();
        let headers = headers.to_vec();
        let table_name = table_name.to_string();
        let len_error = len_error.clone();
        let batch_count = current_batch.len();
        
        let future = tokio::spawn(async move {
            if let Err(e) = insert_batch(&pool, &headers, current_batch, len_error.clone(), &table_name).await {
                eprintln!("Error inserting final batch: {}", e);
            }
        });
        active_futures.push(future);
        total_processed += batch_count as u64;
    }
    
    // Wait for all remaining tasks to complete
    try_join_all(active_futures).await?;
    
    // Finish progress bar
    progress_bar.set_position(total_processed);
    progress_bar.finish_with_message(format!("Completed! Final memory: {}", get_memory_usage()));
    
    let elapsed = start_time.elapsed();
    let records_per_sec = total_processed as f64 / elapsed.as_secs_f64();
    
    println!("=== Processing Summary ===");
    println!("Total records processed: {}", total_processed);
    println!("Time elapsed: {:.2?}", elapsed);
    println!("Average rate: {:.0} records/second", records_per_sec);
    println!("File size: {:.2} MB", file_size_mb);
    println!("Final memory usage: {}", get_memory_usage());

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

// New batch insert function for better performance
pub async fn insert_batch(pool: &sqlx::PgPool, headers: &[String], records: Vec<StringRecord>, errors: Arc<Mutex<Vec<Vec<String>>>>, table_name: &str) -> Result<(), sqlx::Error> {
    if records.is_empty() {
        return Ok(());
    }

    let columns = headers
        .iter()
        .map(|h| String::from(h))
        .collect::<Vec<_>>()
        .join(", ");

    let mut valid_records = Vec::new();
    
    // Filter out invalid records
    for record in records {
        let values: Vec<String> = record.iter()
            .map(|s| s.to_string())
            .collect();

        if values.len() != headers.len() {
            if let Ok(mut errors_guard) = errors.lock() {
                errors_guard.push(values);
            }
            continue;
        }
        valid_records.push(values);
    }

    if valid_records.is_empty() {
        return Ok(());
    }

    // Build batch insert query
    let values_placeholders: Vec<String> = valid_records
        .iter()
        .enumerate()
        .map(|(row_idx, row)| {
            let row_placeholders: Vec<String> = (0..row.len())
                .map(|col_idx| format!("${}", row_idx * row.len() + col_idx + 1))
                .collect();
            format!("({})", row_placeholders.join(", "))
        })
        .collect();

    let query_str = format!(
        "INSERT INTO {} ({}) VALUES {}",
        table_name,
        columns,
        values_placeholders.join(", ")
    );

    // Bind all values
    let mut query = sqlx::query(&query_str);
    for row in &valid_records {
        for value in row {
            query = query.bind(value);
        }
    }

    query.execute(pool).await?;
    Ok(())
}

/// Converts a modified files CSV to a PostgreSQL database table.
///
/// This function reads a CSV file containing modified file data and inserts it into a PostgreSQL
/// database table called "modified_files". It handles the complete workflow of database connection,
/// table creation, and data insertion.
///
/// # Arguments
///
/// * `file_path` - Path to the CSV file containing modified files data
///
/// # Returns
///
/// * `Result<()>` - Success or error result of the conversion operation
///
/// # Environment Variables Required
///
/// * `DATABASE_URL` - PostgreSQL connection string (loaded from .env file)
///
/// # CSV Format Expected
///
/// The CSV file should use comma (`,`) delimiter and include headers. The table schema
/// is automatically determined from the CSV headers.
///
/// # Database Operations
///
/// 1. Establishes connection pool with up to 20 connections
/// 2. Creates table based on CSV headers if it doesn't exist
/// 3. Processes and inserts all CSV data
/// 4. Reports progress and any errors encountered
///
/// # Example
///
/// ```rust
/// use altered_history_analysis::models;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     models::convert_modified_files("../data/modified_files.csv").await?;
///     Ok(())
/// }
/// ```
pub async fn convert_modified_files(file_path: &str) -> Result<()>{
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(20)  // Increased for parallel processing
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

/// Converts multiple CSV files from a directory to a PostgreSQL database table.
///
/// This function processes all CSV files in a specified directory and combines them into
/// a single PostgreSQL table called "altered_histories". It's designed to handle multiple
/// files that share the same schema and aggregate them into one database table.
///
/// # Arguments
///
/// * `directory_path` - Path to the directory containing CSV files to process
///
/// # Returns
///
/// * `Result<()>` - Success or error result of the conversion operation
///
/// # Environment Variables Required
///
/// * `DATABASE_URL` - PostgreSQL connection string (loaded from .env file)
///
/// # CSV Format Expected
///
/// - CSV files should use semicolon (`;`) delimiter
/// - All CSV files in the directory should have the same header structure
/// - Only files with `.csv` extension are processed
///
/// # Database Operations
///
/// 1. Establishes connection pool with up to 20 connections
/// 2. Scans directory for CSV files
/// 3. Creates table schema based on first CSV file's headers
/// 4. Processes each CSV file sequentially with progress tracking
/// 5. Inserts all data into the "altered_histories" table
///
/// # Progress Tracking
///
/// Displays a progress bar showing the number of files processed out of total files found.
///
/// # Example
///
/// ```rust
/// use altered_history_analysis::models;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     models::convert_altered_histories("./data/results/").await?;
///     Ok(())
/// }
/// ```
pub async fn convert_altered_histories(directory_path: &str) -> Result<()>{
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(20)  // Increased for parallel processing
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
        .max_connections(30)  // Increased for parallel processing
        .acquire_timeout(std::time::Duration::from_secs(30))
        .idle_timeout(std::time::Duration::from_secs(600))
        .max_lifetime(std::time::Duration::from_secs(1800))
        .connect(&database_url)
        .await
        .context("Failed to create pool.")?;

    println!("Connected to the database!");

    // Optimize PostgreSQL for bulk insert
    if let Err(e) = optimize_postgres_for_bulk_insert(&pool).await {
        eprintln!("Warning: Could not optimize PostgreSQL settings: {}", e);
    }

    let headers = get_csv_headers(file_path, b',')?;
    let table_name = "altered_histories_clean";
    create_table(&pool, &headers, table_name).await?;
    println!("{}: {:?}", table_name, headers);

    // Process the large CSV file
    let result = process_csv_file(&pool, &headers, b',', file_path, table_name).await;

    // Restore PostgreSQL settings
    if let Err(e) = restore_postgres_settings(&pool).await {
        eprintln!("Warning: Could not restore PostgreSQL settings: {}", e);
    }

    // Run VACUUM ANALYZE after bulk insert for optimal performance
    println!("Running VACUUM ANALYZE to optimize table statistics...");
    let vacuum_query = format!("VACUUM ANALYZE {}", table_name);
    if let Err(e) = sqlx::query(&vacuum_query).execute(&pool).await {
        eprintln!("Warning: VACUUM ANALYZE failed: {}", e);
    } else {
        println!("VACUUM ANALYZE completed successfully");
    }

    result
}

// PostgreSQL optimization settings for bulk inserts
pub async fn optimize_postgres_for_bulk_insert(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    // Temporarily disable synchronous commit for better performance
    sqlx::query("SET synchronous_commit = OFF").execute(pool).await?;
    
    // Increase work_mem for larger sorts
    sqlx::query("SET work_mem = '256MB'").execute(pool).await?;
    
    // Increase maintenance_work_mem for index creation
    sqlx::query("SET maintenance_work_mem = '1GB'").execute(pool).await?;
    
    // Disable autovacuum temporarily during bulk insert
    sqlx::query("SET autovacuum = OFF").execute(pool).await?;
    
    println!("PostgreSQL optimized for bulk insert operations");
    Ok(())
}

// Restore PostgreSQL settings after bulk insert
pub async fn restore_postgres_settings(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SET synchronous_commit = ON").execute(pool).await?;
    sqlx::query("RESET work_mem").execute(pool).await?;
    sqlx::query("RESET maintenance_work_mem").execute(pool).await?;
    sqlx::query("SET autovacuum = ON").execute(pool).await?;
    
    println!("PostgreSQL settings restored to defaults");
    Ok(())
}

// Memory monitoring utility
fn get_memory_usage() -> String {
    match std::fs::read_to_string("/proc/self/status") {
        Ok(status) => {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    return line.to_string();
                }
            }
            "Memory info not found".to_string()
        }
        Err(_) => "Unable to read memory info".to_string()
    }
}

// Get file size for progress estimation
fn get_file_size(file_path: &str) -> u64 {
    std::fs::metadata(file_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

// Get available system memory in MB
fn get_available_memory_mb() -> u64 {
    match std::fs::read_to_string("/proc/meminfo") {
        Ok(content) => {
            for line in content.lines() {
                if line.starts_with("MemAvailable:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            return kb / 1024; // Convert KB to MB
                        }
                    }
                }
            }
            8192 // Default to 8GB if not found
        }
        Err(_) => 8192 // Default fallback
    }
}

// Calculate optimal batch parameters based on system resources
fn calculate_batch_params(file_size_mb: f64) -> (usize, usize) {
    let available_mem_mb = get_available_memory_mb();
    
    // For very large files (>10GB), use smaller batches and higher concurrency
    // For smaller files, use larger batches and lower concurrency
    let (batch_size, max_concurrent) = if file_size_mb > 10000.0 {
        // Large file: smaller batches, more concurrency
        (500, 20)
    } else if file_size_mb > 1000.0 {
        // Medium file: medium batches
        (1000, 15)
    } else {
        // Small file: larger batches, less concurrency
        (2000, 10)
    };
    
    // Adjust based on available memory
    let memory_factor = (available_mem_mb as f64 / 8192.0).min(2.0); // Cap at 2x
    let adjusted_concurrent = ((max_concurrent as f64 * memory_factor) as usize).max(5);
    
    println!("System info: {:.0} MB available memory", available_mem_mb);
    println!("Batch config: {} records/batch, max {} concurrent batches", batch_size, adjusted_concurrent);
    
    (batch_size, adjusted_concurrent)
}

pub async fn perso_query(file_path: &str) -> Result<()>{
    #[derive(sqlx::FromRow, Serialize)]
    pub struct DefaultNix {
        pub origin: String,
        pub revision: String,
        pub path: String,
        pub status: String,
    }
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(20)  // Increased for parallel processing
        .connect(&database_url)
        .await
        .context("Failed to create pool.")?;

    println!("Connected to the database!");

    let query = format!("SELECT origin, revision, path, status FROM modified_files WHERE path like '%/default.nix'");

    let results: Vec<DefaultNix> = sqlx::query_as(&query).fetch_all(&pool).await?;

    // Create a CSV file to store the default.nix results
    let mut wtr = csv::WriterBuilder::new().from_path(file_path)?;

    // Write the header
    wtr.write_record(&["origin", "revision", "path", "status"])?;

    let len = results.len();
    // Write each result
    for result in results {
        wtr.serialize(result)?;
    }

    // Flush and finalize the CSV file
    wtr.flush()?;
    println!("Successfully wrote {} records to {}", len, file_path);

    Ok(())
}



// #[allow(dead_code)]
// pub async fn get_user(pool: &sqlx::PgPool, user_id: i32) -> Result<User, sqlx::Error> {
//     let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
//         .bind(user_id)
//         .fetch_one(pool)
//         .await?;
//     Ok(user)
// }