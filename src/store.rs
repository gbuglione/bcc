use super::common::*;
use super::transaction::Transaction;
use serde::{Deserialize, Serialize};
use sled::{Config, Db};
use thiserror::Error;

#[derive(Serialize, Deserialize)]
pub enum Tx {
    Undisputed(Transaction),
    Disputed(Transaction),
    // In theory there are 3 possible status for transactions: Undisputed, Disputed and Resolved, but assuming
    // transactions cannot be disputed more than once, we can remove already resolved transactions from the db
    // since we're not going to need them anymore
}

/// Keep a record of validated transactions to process disputes.
///
/// In essence, adopt a multi-tiered system for transaction bookkeeping to avoid
/// having everything in memory.
/// At the moment it's just an embedded database because it was easy to add, but of course we pay
/// the price of ser/de even for objects that are in the db cache and a custom in-memory ds may be better.
/// It's probably useful to look at performance in more detail for production systems.
///
/// If the possibility to dispute transactions expire after some time, remove such
/// transactions from the system.
#[derive(Clone)]
pub struct TransactionStore {
    db: Db,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("not found")]
    NotFound,
    #[error("transaction not available for dispute")]
    NotAvailableForDispute,
    #[error("transaction not in dispute")]
    NoDisputeActive,
    #[error(transparent)]
    Db(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
}

impl TransactionStore {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            db: Config::new().temporary(true).open()?,
        })
    }

    /// Insert a new transaction in the database.
    pub fn insert(&mut self, id: TxId, tx: Transaction) -> Result<(), Error> {
        debug_assert!(matches!(
            tx,
            Transaction::Deposit { .. } | Transaction::Withdrawal { .. }
        ));
        self.db.insert(
            id.to_le_bytes(),
            bincode::serialize(&Tx::Undisputed(tx)).unwrap(),
        )?;
        Ok(())
    }

    fn update_tx<F>(&self, id: TxId, mut f: F) -> Result<Tx, Error>
    where
        F: FnMut(Tx) -> Option<Tx>,
    {
        // bincode was chosen for no particular reason besides being a well known format
        // focused on speed and footprint.
        // std::mem::transmute could have been used as well if latency was the primary concern
        // but it's unsafe so extra care must be exercised
        // do not handle corruptions for now
        Ok(bincode::deserialize(
            &self
                .db
                .fetch_and_update(id.to_le_bytes(), |maybe_tx| {
                    maybe_tx.and_then(|raw_tx| {
                        f(bincode::deserialize(raw_tx).unwrap())
                            .map(|tx| bincode::serialize(&tx).unwrap())
                    })
                })?
                .ok_or(Error::NotFound)?,
        )
        .unwrap())
    }

    pub fn fetch_dispute(&mut self, id: TxId) -> Result<Transaction, Error> {
        let tx = self.update_tx(id, |tx| match tx {
            Tx::Undisputed(tx) => Some(Tx::Disputed(tx)),
            other => Some(other), // this has one serialization step more than necessary
        })?;

        match tx {
            Tx::Undisputed(inner) => Ok(inner),
            _ => Err(Error::NotAvailableForDispute),
        }
    }

    pub fn fetch_resolve(&mut self, id: TxId) -> Result<Transaction, Error> {
        let tx = self.update_tx(id, |tx| match tx {
            Tx::Disputed(_) => None, // assume a transaction can only be disputed once
            other => Some(other),    // this has one serialization step more than necessary
        })?;

        match tx {
            Tx::Disputed(inner) => Ok(inner),
            _ => Err(Error::NoDisputeActive),
        }
    }
}
