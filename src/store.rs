use super::common::*;
use super::transaction::TxStatus;
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};

const TX_TABLE: TableDefinition<u64, TxRecord> = TableDefinition::new("transactions");
use thiserror::Error;

/// Keep a record of validated transactions to process disputes.
///
/// In essence, adopt a multi-tiered system for transaction bookkeeping to avoid
/// having everything in memory.
/// At the moment it's just an embedded database because it was easy to add,
/// but a custom ds may be better.
/// It's probably useful to look at performance in more detail for production systems.
///
/// If the possibility to dispute transactions expire after some time, remove such
/// transactions from the system.
#[derive(Debug)]
pub struct TransactionStore {
    db: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxRecord {
    pub value: Value,
    pub status: TxStatus,
}

impl redb::Value for TxRecord {
    type SelfType<'a> = Self;
    type AsBytes<'a> = [u8; 17];

    fn fixed_width() -> Option<usize> {
        Some(17)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let value = Value::deserialize(data[0..16].try_into().expect("Invalid length for Value"));
        let status = match data[16] {
            0 => TxStatus::Undisputed,
            1 => TxStatus::Disputed,
            _ => panic!("Invalid status byte"),
        };
        Self { value, status }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut bytes = [0u8; 17];
        bytes[0..16].copy_from_slice(&value.value.serialize());
        bytes[16] = value.status as u8;
        bytes
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("TxRecord")
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("not found")]
    NotFound,
    #[error(transparent)]
    Db(Box<redb::Error>),
    #[error("error creating temporary file")]
    TempFile(std::io::Error),
}

// db methods return individual errors, instead of exposing all the redb errors
// to the user return onnly a top level one
impl<E> From<E> for Error
where
    redb::Error: From<E>,
{
    fn from(e: E) -> Self {
        Self::Db(Box::new(redb::Error::from(e)))
    }
}

impl Default for TransactionStore {
    fn default() -> Self {
        Self::new().expect("Failed to create default TransactionStore")
    }
}

impl TransactionStore {
    pub fn new() -> Result<Self, Error> {
        let db = Database::builder().create_file(tempfile::tempfile().map_err(Error::TempFile)?)?;
        Ok(Self { db })
    }

    fn compute_id(client: Client, tx_id: TxId) -> u64 {
        // Use the client id as the high bits and the tx id as the low bits.
        // This allows us to do prefix queries on clients.
        ((client as u64) << 32) | (tx_id as u64)
    }

    /// Insert a new transaction in the database.
    pub fn insert(&self, client: Client, tx_id: TxId, record: TxRecord) -> Result<(), Error> {
        let id = Self::compute_id(client, tx_id);
        let mut write_txn = self.db.begin_write()?;
        // Avoid flushing to disk, this is just a temporary store.
        write_txn.set_durability(redb::Durability::None);
        {
            let mut table = write_txn.open_table(TX_TABLE)?;
            table.insert(id, record)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn remove(&self, client: Client, tx_id: TxId) -> Result<(), Error> {
        let id = Self::compute_id(client, tx_id);
        let mut write_txn = self.db.begin_write()?;
        // Avoid flushing to disk, this is just a temporary store.
        write_txn.set_durability(redb::Durability::None);
        {
            let mut table = write_txn.open_table(TX_TABLE)?;
            table.remove(id)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Fetch a transaction
    pub fn get(&self, client: Client, tx_id: TxId) -> Result<TxRecord, Error> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TX_TABLE)?;
        let id = Self::compute_id(client, tx_id);
        let tx = table.get(id)?.ok_or(Error::NotFound)?;
        Ok(tx.value().clone())
    }
}
