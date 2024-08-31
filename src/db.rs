use sqlx::prelude::FromRow;
use sqlx::{Executor, SqliteConnection, SqlitePool};
use tokio_stream::Stream;

#[derive(Debug, Clone, Copy, FromRow, PartialEq)]
pub struct Client {
    pub id: u16,
    pub available_funds: f64,
    pub held_funds: f64,
    pub total_funds: f64,
    pub locked: bool,
}

#[derive(Debug, FromRow, PartialEq)]
pub struct Dispute {
    pub client_id: u16,
    pub tx_id: u32,
    pub amount: f64,
}

#[derive(Debug, FromRow, PartialEq)]
pub struct Transaction {
    pub id: u32,
    pub client_id: u16,
    pub transaction_type: String,
    pub amount: f64,
}

pub async fn create_client(conn: &mut SqliteConnection, client: Client) -> sqlx::Result<Client> {
    static QUERY: &str = "
        INSERT INTO client (id, locked, total_funds, available_funds, held_funds)
        VALUES (?, ?, ?, ?, ?)
        RETURNING *
    ";
    let row = sqlx::query_as::<_, Client>(QUERY)
        .bind(client.id)
        .bind(client.locked)
        .bind(client.total_funds)
        .bind(client.available_funds)
        .bind(client.held_funds)
        .fetch_one(conn)
        .await?;
    Ok(row)
}

pub async fn insert_transaction(
    conn: &mut SqliteConnection,
    transaction: Transaction,
) -> sqlx::Result<Transaction> {
    static QUERY: &str = "
        INSERT INTO tx (id, client_id, transaction_type, amount) 
        VALUES (?, ?, ?, ?)
        RETURNING *
    ";
    let row = sqlx::query_as::<_, Transaction>(QUERY)
        .bind(transaction.id)
        .bind(transaction.client_id)
        .bind(transaction.transaction_type)
        .bind(transaction.amount)
        .fetch_one(conn)
        .await?;
    Ok(row)
}

pub fn get_clients(conn: &SqlitePool) -> impl Stream<Item = sqlx::Result<Client>> + '_ {
    static QUERY: &str = "SELECT * FROM client ORDER BY id";
    sqlx::query_as::<_, Client>(QUERY).fetch(conn)
}

pub async fn insert_dispute(
    conn: &mut SqliteConnection,
    dispute: Dispute,
) -> sqlx::Result<Dispute> {
    static QUERY: &str = "
        INSERT INTO dispute (client_id, tx_id, amount) 
        VALUES (?, ?, ?)
        RETURNING *
    ";
    let row = sqlx::query_as::<_, Dispute>(QUERY)
        .bind(dispute.client_id)
        .bind(dispute.tx_id)
        .bind(dispute.amount)
        .fetch_one(conn)
        .await?;
    Ok(row)
}

pub async fn get_dispute(
    conn: &mut SqliteConnection,
    transaction_id: u32,
    client_id: u16,
) -> sqlx::Result<Option<Dispute>> {
    static QUERY: &str = "SELECT * FROM dispute WHERE tx_id = ? AND client_id = ?";
    let row = sqlx::query_as::<_, Dispute>(QUERY)
        .bind(transaction_id)
        .bind(client_id)
        .fetch_optional(conn)
        .await?;
    Ok(row)
}

pub async fn delete_dispute(
    conn: &mut SqliteConnection,
    transaction_id: u32,
    client_id: u16,
) -> sqlx::Result<()> {
    static QUERY: &str = "DELETE FROM dispute WHERE tx_id = ? AND client_id = ?";
    sqlx::query(QUERY)
        .bind(transaction_id)
        .bind(client_id)
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn update_client(conn: &mut SqliteConnection, client: Client) -> sqlx::Result<Client> {
    static QUERY: &str = "
        UPDATE client
        SET locked = ?, total_funds = ?, held_funds = ?, available_funds = ?
        WHERE id = ?
        RETURNING *
    ";
    let row = sqlx::query_as::<_, Client>(QUERY)
        .bind(client.locked)
        .bind(client.total_funds)
        .bind(client.held_funds)
        .bind(client.available_funds)
        .bind(client.id)
        .fetch_one(conn)
        .await?;
    Ok(row)
}

/// Get a client by id, creates one if it doesn't exist
pub async fn get_client(conn: &mut SqliteConnection, id: u16) -> sqlx::Result<Client> {
    static QUERY: &str = "SELECT * FROM client WHERE id = ?";
    let row = sqlx::query_as::<_, Client>(QUERY)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;
    if let Some(row) = row {
        Ok(row)
    } else {
        create_client(
            conn,
            Client {
                id,
                locked: false,
                total_funds: 0.0,
                available_funds: 0.0,
                held_funds: 0.0,
            },
        )
        .await
    }
}

pub async fn get_transaction(
    conn: &mut SqliteConnection,
    transaction_id: u32,
    client_id: u16,
) -> sqlx::Result<Option<Transaction>> {
    static QUERY: &str = "SELECT * FROM tx WHERE id = ? AND client_id = ?";
    let rows = sqlx::query_as::<_, Transaction>(QUERY)
        .bind(transaction_id)
        .bind(client_id)
        .fetch_optional(conn)
        .await?;
    Ok(rows)
}

pub async fn create_tables(conn: &SqlitePool) -> sqlx::Result<()> {
    conn.execute(
        "CREATE TABLE client (
            id INTEGER PRIMARY KEY,
            locked BOOLEAN NOT NULL DEFAULT FALSE,
            total_funds REAL NOT NULL DEFAULT 0.0,
            available_funds REAL NOT NULL DEFAULT 0.0,
            held_funds REAL NOT NULL DEFAULT 0.0
        );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE tx (
            id BIGINT,
            client_id INTEGER NOT NULL REFERENCES client(id),
            transaction_type TEXT NOT NULL,
            amount REAL NOT NULL,
            PRIMARY KEY (id, client_id)
        );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE dispute (
            client_id INTEGER NOT NULL,
            tx_id BIGINT NOT NULL,
            amount REAL NOT NULL,
            PRIMARY KEY (client_id, tx_id),
            FOREIGN KEY (tx_id, client_id) REFERENCES tx(id, client_id)
        );
        ",
    )
    .await?;
    Ok(())
}
