mod db;
mod engine;

use sqlx::sqlite::SqliteConnectOptions;
use std::env;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let input_file = env::args().nth(1).expect("Expected input file");
    let file = tokio::fs::File::open(input_file)
        .await
        .expect("Input file not found");

    let db_name = format!("{}.db", Uuid::new_v4());
    let conn = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&db_name)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    db::create_tables(&conn).await.unwrap();

    let transaction_stream = csv_async::AsyncReaderBuilder::new()
        .flexible(true)
        .create_deserializer(file)
        .into_deserialize();

    // Process transaction stream with SQLite storage backend
    let mut engine = engine::PaymentEngine::new(conn.clone(), 3, 10);
    engine.process_transactions(transaction_stream).await;
    engine.stop().await;
    // Output to stdout
    let mut writer = csv_async::AsyncWriter::from_writer(tokio::io::stdout());
    writer
        .write_record(&["client", "available", "held", "total", "locked"])
        .await
        .unwrap();
    let clients = db::get_clients(&conn);
    tokio::pin!(clients);
    while let Some(client) = clients.next().await {
        let client = client.unwrap();
        writer
            .write_record(&[
                client.id.to_string(),
                format!("{:.4}", client.available_funds),
                format!("{:.4}", client.held_funds),
                format!("{:.4}", client.total_funds),
                client.locked.to_string(),
            ])
            .await
            .unwrap();
    }
    tokio::fs::remove_file(db_name).await.unwrap();
}
