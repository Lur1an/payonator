use monostate::MustBe;
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, SqlitePool};
use std::error::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt};

use crate::db::{self, Dispute};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum TransactionType {
    Deposit {
        #[serde(rename = "type")]
        type_name: MustBe!("deposit"),
        amount: f64,
    },
    Withdrawal {
        #[serde(rename = "type")]
        type_name: MustBe!("withdrawal"),
        amount: f64,
    },
    Dispute {
        #[serde(rename = "type")]
        type_name: MustBe!("dispute"),
    },
    ChargeBack {
        #[serde(rename = "type")]
        type_name: MustBe!("chargeback"),
    },
    Resolve {
        #[serde(rename = "type")]
        type_name: MustBe!("resolve"),
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct TransactionRecord {
    client: u16,
    tx: u32,
    #[serde(flatten)]
    transaction_type: TransactionType,
}

pub struct PaymentEngine {
    worker_tx: Vec<mpsc::Sender<TransactionRecord>>,
    tasks: Vec<JoinHandle<()>>,
}

impl PaymentEngine {
    pub fn new(conn: SqlitePool, workers: usize, worker_buffer: usize) -> PaymentEngine {
        let mut tasks = Vec::with_capacity(workers);
        let mut worker_tx = Vec::with_capacity(workers);
        (0..workers).for_each(|_| {
            let conn = conn.clone();
            let (tx, mut rx) = mpsc::channel(worker_buffer);
            let task = tokio::spawn(async move {
                while let Some(record) = rx.recv().await {
                    let _ = process_transaction(&conn, record).await;
                }
            });
            tasks.push(task);
            worker_tx.push(tx);
        });
        PaymentEngine { worker_tx, tasks }
    }

    // Stops the engine and waits for all tasks to exit
    pub async fn stop(self) {
        // Drop the `Sender` so the workers can exit after exhausting the buffered records
        drop(self.worker_tx);
        for task in self.tasks {
            task.await.unwrap();
        }
    }

    pub async fn process_transactions(
        &mut self,
        transactions: impl Stream<Item = Result<TransactionRecord, impl Into<BoxError>>>,
    ) {
        // Create workers
        tokio::pin!(transactions);
        let workers = self.worker_tx.len();
        while let Some(transaction) = transactions.next().await {
            if let Ok(transaction) = transaction {
                self.worker_tx[transaction.client as usize % workers]
                    .send(transaction)
                    .await
                    .expect("worker terminated")
            }
        }
    }
}

async fn process_transaction(pool: &SqlitePool, record: TransactionRecord) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let mut client = db::get_client(&mut conn, record.client).await?;
    if client.locked {
        anyhow::bail!("client {} is locked", client.id)
    }
    match record.transaction_type {
        TransactionType::Deposit { amount, .. } => {
            client.available_funds += amount;
            client.total_funds += amount;
            let mut tx = conn.begin().await?;
            db::insert_transaction(
                &mut tx,
                db::Transaction {
                    id: record.tx,
                    client_id: record.client,
                    transaction_type: "deposit".to_owned(),
                    amount,
                },
            )
            .await?;
            db::update_client(&mut tx, client).await?;
            tx.commit().await?;
        }
        TransactionType::Withdrawal { amount, .. } => {
            if client.available_funds < amount {
                anyhow::bail!("insufficient available funds for withdrawal");
            }
            client.available_funds -= amount;
            client.total_funds -= amount;
            let mut tx = conn.begin().await?;
            db::insert_transaction(
                &mut tx,
                db::Transaction {
                    id: record.tx,
                    client_id: record.client,
                    transaction_type: "withdrawal".to_owned(),
                    amount,
                },
            )
            .await?;
            db::update_client(&mut tx, client).await?;
            tx.commit().await?;
        }
        TransactionType::Dispute { .. } => {
            let Some(disputed_transaction) =
                db::get_transaction(&mut conn, record.tx, record.client).await?
            else {
                anyhow::bail!(
                    "disputed_transaction {} does not exist for client {}",
                    record.tx,
                    record.client
                );
            };
            if disputed_transaction.transaction_type != "deposit" {
                anyhow::bail!("cannot dispute non-deposit transaction");
            }
            client.available_funds -= disputed_transaction.amount;
            client.held_funds += disputed_transaction.amount;

            let mut tx = conn.begin().await?;
            db::insert_dispute(
                &mut tx,
                Dispute {
                    client_id: record.client,
                    tx_id: record.tx,
                    amount: disputed_transaction.amount,
                },
            )
            .await?;
            db::update_client(&mut tx, client).await?;
            tx.commit().await?;
        }
        TransactionType::ChargeBack { .. } => {
            let Some(disputed_transaction) =
                db::get_dispute(&mut conn, record.tx, record.client).await?
            else {
                anyhow::bail!(
                    "disputed_transaction {} does not exist for client {}",
                    record.tx,
                    record.client
                );
            };
            client.total_funds -= disputed_transaction.amount;
            client.held_funds -= disputed_transaction.amount;
            if client.total_funds < 0.0 {
                anyhow::bail!("can't chargeback to negative total funds");
            }
            client.locked = true;

            let mut tx = conn.begin().await?;
            db::update_client(&mut tx, client).await?;
            db::delete_dispute(&mut tx, record.tx, record.client).await?;
            tx.commit().await?;
        }
        TransactionType::Resolve { .. } => {
            let Some(disputed_transaction) =
                db::get_dispute(&mut conn, record.tx, record.client).await?
            else {
                anyhow::bail!(
                    "disputed_transaction {} does not exist for client {}",
                    record.tx,
                    record.client
                );
            };
            client.available_funds += disputed_transaction.amount;
            client.held_funds -= disputed_transaction.amount;

            let mut tx = conn.begin().await?;
            db::update_client(&mut tx, client).await?;
            db::delete_dispute(&mut tx, record.tx, record.client).await?;
            tx.commit().await?;
        }
    };
    Ok(())
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use self::db::{get_clients, Client};
    use super::*;
    use pretty_assertions::assert_eq;

    fn dispute(client: u16, tx: u32) -> TransactionRecord {
        TransactionRecord {
            client,
            tx,
            transaction_type: TransactionType::Dispute {
                type_name: MustBe!("dispute"),
            },
        }
    }

    fn withdrawal(client: u16, tx: u32, amount: f64) -> TransactionRecord {
        TransactionRecord {
            client,
            tx,
            transaction_type: TransactionType::Withdrawal {
                type_name: MustBe!("withdrawal"),
                amount,
            },
        }
    }

    fn deposit(client: u16, tx: u32, amount: f64) -> TransactionRecord {
        TransactionRecord {
            client,
            tx,
            transaction_type: TransactionType::Deposit {
                type_name: MustBe!("deposit"),
                amount,
            },
        }
    }

    fn resolve(client: u16, tx: u32) -> TransactionRecord {
        TransactionRecord {
            client,
            tx,
            transaction_type: TransactionType::Resolve {
                type_name: MustBe!("resolve"),
            },
        }
    }

    fn chargeback(client: u16, tx: u32) -> TransactionRecord {
        TransactionRecord {
            client,
            tx,
            transaction_type: TransactionType::ChargeBack {
                type_name: MustBe!("chargeback"),
            },
        }
    }

    #[tokio::test]
    async fn test_process_transaction() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        db::create_tables(&pool).await.unwrap();
        let transactions = [
            deposit(1, 1, 1.0),
            deposit(1, 4, 3.0),
            dispute(1, 1),
            dispute(1, 4),
            resolve(1, 1),
            dispute(1, 1),
            chargeback(1, 1),
        ];
        for transaction in transactions {
            process_transaction(&pool, transaction).await.unwrap();
        }
        let withdrawal_error = process_transaction(&pool, withdrawal(1, 6, 3.0)).await;
        assert!(withdrawal_error.is_err());
        let mut conn = pool.acquire().await.unwrap();
        let client = db::get_client(&mut conn, 1).await.unwrap();
        assert_eq!(client.available_funds, 0.0);
        assert_eq!(client.held_funds, 3.0);
        assert_eq!(client.total_funds, 3.0);
    }

    #[tokio::test]
    async fn test_account_locking() -> anyhow::Result<()> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        db::create_tables(&pool).await?;

        let mut conn = pool.acquire().await?;
        let clients = vec![
            db::create_client(
                &mut conn,
                Client {
                    id: 0,
                    locked: true,
                    total_funds: 100.0,
                    available_funds: 100.0,
                    held_funds: 0.0,
                },
            )
            .await?,
            db::create_client(
                &mut conn,
                Client {
                    id: 1,
                    locked: true,
                    total_funds: 150.0,
                    available_funds: 150.0,
                    held_funds: 0.0,
                },
            )
            .await?,
        ];
        let mut engine = PaymentEngine::new(pool.clone(), 1, 5);
        let transactions = tokio_stream::iter([
            withdrawal(1, 1, 100.0),
            deposit(1, 2, 100.0),
            dispute(1, 2),
            withdrawal(1, 5, 100.0),
            deposit(0, 6, 100.0),
            chargeback(1, 7),
            dispute(1, 2),
            withdrawal(1, 9, 100.0),
            deposit(1, 10, 100.0),
            dispute(1, 10),
            chargeback(1, 10),
        ]);
        engine
            .process_transactions(transactions.map(Ok::<_, Infallible>))
            .await;
        engine.stop().await;
        let stream = get_clients(&pool);
        tokio::pin!(stream);
        let mut updated_clients = vec![];
        while let Some(client) = stream.next().await {
            updated_clients.push(client?);
        }
        assert_eq!(clients, updated_clients);
        Ok(())
    }
}
