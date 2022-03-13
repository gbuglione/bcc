use super::{
    account::{self, Account, AccountInner, Active},
    common::*,
    store::{self, TransactionStore, TxRecord},
    transaction::{
        Transaction::{self, *},
        TxStatus,
    },
};
use std::thread::JoinHandle;
use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, SyncSender},
};
use thiserror::Error;

const BUF_SIZE: usize = 100;

pub struct Engine {
    workers: Vec<WorkerHandle>,
}

impl Engine {
    /// Construct a new engine to process transactions
    /// n_workers constrols the amount of parallelism it will try to exploit
    pub fn new(n_workers: usize) -> Result<Self, Error> {
        let workers = (0..n_workers)
            .map(|_| {
                let (worker, tx) = Worker::new()?;
                let handle = worker.run();
                Ok::<_, Error>(WorkerHandle { tx, handle })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { workers })
    }

    /// Process one transaction a' la sans I/O
    /// A nice improvement on this would be a work-stealing mechanism to better balance
    /// queues like rayon/cilk.
    /// Such mechanism would work on the premises above: since a worker is working on one transaction at a time, every
    /// transaction in its queue which does not belong to the same account can be worked on concurrently and can be stolen
    /// by any other worker, provided the other worker steals all transactions belonging to the same client in the queue
    /// and the associated account state (unless a shared access ds is used).
    ///
    /// This is an implicit serialization point, if this original order is needed for compliance / accounting
    /// purpose, this is the place to add the functionality (e.g. by saving timestamps / counters for each transaction).
    pub fn feed(&mut self, tx: Transaction) -> Result<(), Error> {
        // we could also use a more "fair" sharding strategy like id hashing, this is a starting point
        let worker_id = tx.client() as usize % self.workers.len();
        Ok(self.workers[worker_id].tx.send(tx)?)
    }

    /// Wait for all transactions to be processed
    pub fn finish(self) -> Result<Accounts, Error> {
        Ok(self
            .workers
            .into_iter()
            .flat_map(|WorkerHandle { tx, handle }| {
                drop(tx);
                handle.join().unwrap().accounts.into_iter()
            })
            .collect())
    }

    /// Run the engine on the incoming stream of transactions.
    pub fn run<S: Iterator<Item = Transaction>>(mut self, input: S) -> Result<Accounts, Error> {
        // Only partial order is needed for handling transactions, that is, transactions belonging
        // to different clients are assumed to be indipendent given input data representation.
        // Workers may reorder transactions in a way that is consistent with the assumptions above
        // to increase performance without any impact on the final state.
        for tx in input {
            self.feed(tx)?;
        }
        self.finish()
    }
}

struct WorkerHandle {
    tx: SyncSender<Transaction>,
    handle: JoinHandle<State>,
}

// Shard work based on account id, assuming transactions are independent
struct Worker {
    rx: Receiver<Transaction>,
    state: State,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] store::Error),
    #[error("something went wrong internally")]
    Mpsc(#[from] mpsc::SendError<Transaction>),
    #[error("account not found")]
    AccountNotFound,
    #[error("account frozen")]
    AccountFrozen,
    #[error(transparent)]
    Account(#[from] account::AccountError),
    #[error("transaction not available for dispute")]
    NotAvailableForDispute,
    #[error("transaction not in dispute")]
    NoDisputeActive,
}

impl Worker {
    pub fn new() -> Result<(Self, SyncSender<Transaction>), Error> {
        let (tx, rx) = std::sync::mpsc::sync_channel(BUF_SIZE);
        Ok((
            Self {
                rx,
                state: State::default(),
            },
            tx,
        ))
    }

    fn process_tx(&mut self, tx: Transaction) -> Result<(), Error> {
        match tx {
            Deposit {
                client,
                value,
                tx_id,
            } => self.state.deposit(client, tx_id, value),
            // Withdrawal are not inserted in the tx store because they cannot be disputed
            Withdrawal { client, value, .. } => self.state.withdraw(client, value),
            Dispute { tx_id, client } => self.state.dispute(client, tx_id),
            Chargeback { tx_id, client } => self.state.resolve(client, tx_id, |account, tx| {
                Ok(account.chargeback(tx.value)?.freeze().into())
            }),
            Resolve { tx_id, client } => self.state.resolve(client, tx_id, |account, tx| {
                Ok(account.release_funds(tx.value)?.into())
            }),
        }
    }

    pub fn run(mut self) -> JoinHandle<State> {
        std::thread::spawn(move || {
            // recv() will only fail on disconnection
            while let Ok(tx) = self.rx.recv() {
                // do not block on errors
                // transactions that result in errors will be ignored and will not put
                // the system in an invalid state
                if let Err(_e) = self.process_tx(tx) {
                    // eprintln!("error while processing tx: {e}"); // some logging machinery would be better suited for this
                }
            }
            self.state
        })
    }
}

/// View on a (sub)set of accounts and their transactions
#[derive(Default, Debug)]
pub struct State {
    // if there are lots of clients this could be a tiered system
    // but it's fine as we only have u16::MAX accounts at most
    accounts: Accounts,
    // Record of transactions issued by clients in this partition
    txs: TransactionStore,
}

pub type Accounts = HashMap<Client, Account>;

impl State {
    fn fetch_account(
        &self,
        client: Client,
        create_on_miss: bool,
    ) -> Result<AccountInner<Active>, Error> {
        if let Some(account) = self.accounts.get(&client) {
            match account {
                Account::Active(inner) => Ok(*inner),
                Account::Frozen(_) => Err(Error::AccountFrozen),
            }
        } else if create_on_miss {
            Ok(AccountInner::default())
        } else {
            Err(Error::AccountNotFound)
        }
    }

    // Fetch account state and specific transaction
    fn fetch_all(
        &self,
        client: Client,
        tx_id: TxId,
    ) -> Result<(AccountInner<Active>, TxRecord), Error> {
        let account = self.fetch_account(client, false)?;
        let tx = self.txs.get(client, tx_id)?;
        Ok((account, tx))
    }

    // Write back account and transaction updates.
    // By doing the db write first, we ensure that the account state is always consistent with transactions,
    // as writing to memory cannot fail.
    fn write_back(
        &mut self,
        client: Client,
        tx_id: TxId,
        account: Account,
        record: Option<TxRecord>,
    ) -> Result<(), Error> {
        if let Some(record) = record {
            self.txs.insert(client, tx_id, record)?;
        } else {
            self.txs.remove(client, tx_id)?;
        }
        self.accounts.insert(client, account);
        Ok(())
    }

    fn deposit(&mut self, client: Client, tx_id: TxId, value: Value) -> Result<(), Error> {
        let new_account = self.fetch_account(client, true)?.deposit(value)?;
        self.write_back(
            client,
            tx_id,
            new_account.into(),
            Some(TxRecord {
                value,
                status: TxStatus::Undisputed,
            }),
        )
    }

    fn withdraw(&mut self, client: Client, value: Value) -> Result<(), Error> {
        let acc = self.fetch_account(client, false)?.withdraw(value)?;
        self.accounts.insert(client, acc.into());
        // withdraws are not stored since they cannot be disputed, see assumptions in README
        Ok(())
    }

    fn dispute(&mut self, client: Client, tx_id: TxId) -> Result<(), Error> {
        let (account, tx) = self.fetch_all(client, tx_id)?;
        if let TxStatus::Undisputed = tx.status {
            let account = account.freeze_funds(tx.value)?;
            self.write_back(
                client,
                tx_id,
                account.into(),
                Some(TxRecord {
                    value: tx.value,
                    status: TxStatus::Disputed,
                }),
            )
        } else {
            Err(Error::NotAvailableForDispute)
        }
    }

    fn resolve<F>(&mut self, client: Client, tx_id: TxId, f: F) -> Result<(), Error>
    where
        F: FnOnce(&AccountInner<Active>, &TxRecord) -> Result<Account, Error>,
    {
        let (account, tx) = self.fetch_all(client, tx_id)?;
        if let TxStatus::Disputed = tx.status {
            let account = f(&account, &tx)?;
            self.write_back(client, tx_id, account, None)
        } else {
            Err(Error::NoDisputeActive)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::*;
    const CLIENT: u16 = 0;

    fn deposit(client: Client, tx_id: TxId, value: Value) -> Transaction {
        Transaction::Deposit {
            client,
            tx_id,
            value,
        }
    }

    fn withdraw(client: Client, tx_id: TxId, value: Value) -> Transaction {
        Transaction::Withdrawal {
            client,
            tx_id,
            value,
        }
    }

    fn dispute(client: Client, tx_id: TxId) -> Transaction {
        Transaction::Dispute { client, tx_id }
    }

    fn resolve(client: Client, tx_id: TxId) -> Transaction {
        Transaction::Resolve { client, tx_id }
    }

    fn chargeback(client: Client, tx_id: TxId) -> Transaction {
        Transaction::Chargeback { client, tx_id }
    }

    #[quickcheck]
    fn test_deposit(tx: Transaction) -> TestResult {
        if let Transaction::Deposit { client, value, .. } = tx {
            TestResult::from_bool(
                Engine::new(1)
                    .unwrap()
                    .run([tx].into_iter())
                    .unwrap()
                    .get(&client)
                    .unwrap()
                    .available()
                    == value,
            )
        } else {
            TestResult::discard()
        }
    }

    #[quickcheck]
    fn test_withdraw(tx: Transaction) -> TestResult {
        if let Transaction::Withdrawal { client, .. } = tx {
            TestResult::from_bool(
                Engine::new(1)
                    .unwrap()
                    .run([tx].into_iter())
                    .unwrap()
                    .get(&client)
                    .is_none(),
            )
        } else {
            TestResult::discard()
        }
    }

    #[test]
    fn test_deposit_withdraw() {
        let mut eng = Worker::new().unwrap().0;
        eng.process_tx(deposit(CLIENT, 0, Value::TEN)).unwrap();
        assert_eq!(eng.state.accounts.get(&CLIENT).unwrap().available(), Value::TEN);
        eng.process_tx(withdraw(CLIENT, 1, Value::ONE)).unwrap();
        assert_eq!(
            eng.state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN - Value::ONE
        );
    }

    #[test]
    fn test_freeze_release() {
        let mut eng = Worker::new().unwrap().0;
        eng.process_tx(deposit(CLIENT, 0, Value::TEN)).unwrap();
        eng.process_tx(deposit(CLIENT, 1, Value::ONE)).unwrap();
        eng.process_tx(dispute(CLIENT, 1)).unwrap();
        assert_eq!(
            eng.state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN
        );
        assert_eq!(eng.state.accounts.get(&CLIENT).unwrap().held(), Value::ONE);
        eng.process_tx(resolve(CLIENT, 1)).unwrap();
        assert_eq!(eng.state.accounts.get(&CLIENT).unwrap().available(), Value::TEN + Value::ONE);
        assert_eq!(eng.state.accounts.get(&CLIENT).unwrap().held(), Value::ZERO);
    }

    #[test]
    fn test_freeze_chargeback() {
        let mut eng = Worker::new().unwrap().0;
        eng.process_tx(deposit(CLIENT, 0, Value::TEN)).unwrap();
        eng.process_tx(deposit(CLIENT, 1, Value::ONE)).unwrap();
        eng.process_tx(dispute(CLIENT, 1)).unwrap();
        assert_eq!(
            eng.state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN
        );
        assert_eq!(eng.state.accounts.get(&CLIENT).unwrap().held(), Value::ONE);
        eng.process_tx(chargeback(CLIENT, 1)).unwrap();
        assert_eq!(
            eng.state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN
        );
        assert_eq!(eng.state.accounts.get(&CLIENT).unwrap().held(), Value::ZERO);
        assert!(matches!(eng.state.accounts.get(&CLIENT).unwrap(), Account::Frozen(_), ));
    }

    #[quickcheck]
    fn test_parallelism_is_correct(batch: Vec<Transaction>) {
        assert_eq!(
            Engine::new(1)
                .unwrap()
                .run(batch.clone().into_iter())
                .unwrap(),
            Engine::new(8).unwrap().run(batch.into_iter()).unwrap()
        );
    }
}
