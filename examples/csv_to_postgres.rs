use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use std::env;

fn main()-> Result<()>{
    let password = env::var("POSTGRES_PASSWORD").context("POSTGRES_PASSWORD env variable not set")?;
    let conn_string = format!("host=localhost user=postgres password={}", password);
    let mut client = Client::connect(&conn_string, NoTls)?;

    client.batch_execute("
        CREATE TABLE person (
            id      SERIAL PRIMARY KEY,
            name    TEXT NOT NULL,
            data    BYTEA
        )
    ")?;

    let name = "Ferris";
    let data = None::<&[u8]>;
    client.execute(
        "INSERT INTO person (name, data) VALUES ($1, $2)",
        &[&name, &data],
    )?;

    for row in client.query("SELECT id, name, data FROM person", &[])? {
        let id: i32 = row.get(0);
        let name: &str = row.get(1);
        let data: Option<&[u8]> = row.get(2);

        println!("found person: {} {} {:?}", id, name, data);
    }
    Ok(())
}