use super::{
    account::{self, Account, AccountInner, Active},
    common::*,
    store::{self, TransactionStore},
    transaction::Transaction::{self, *},
};
use hashbrown::{hash_map::Entry, HashMap};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;
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
        let worker_id = tx.client() as usize % self.workers.len();
        Ok(self.workers[worker_id].tx.send(tx)?)
    }

    /// Wait for all transactions to be processed
    pub fn finish(self) -> Result<LocalState, Error> {
        Ok(self
            .workers
            .into_iter()
            .map(|WorkerHandle { tx, handle }| {
                drop(tx);
                handle.join().unwrap()
            })
            .fold(LocalState::default(), |acc, item| acc.merge(item)))
    }

    /// Run the engine on the incoming stream of transactions.
    pub fn run<S: Iterator<Item = Transaction>>(mut self, input: S) -> Result<LocalState, Error> {
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
    handle: JoinHandle<LocalState>,
}

// Shard work based on account id, assuming transactions are independent
struct Worker {
    rx: Receiver<Transaction>,
    store: TransactionStore,
    local_state: LocalState,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Store(#[from] store::Error),
    #[error("Dispute tx client does not match with disputed tx client")]
    MismatchedClient,
    #[error("something went wrong internally")]
    Mpsc(#[from] mpsc::SendError<Transaction>),
}

impl Worker {
    pub fn new() -> Result<(Self, SyncSender<Transaction>), Error> {
        // TODO: std's mpsc is not the best in term of perf
        let (tx, rx) = std::sync::mpsc::sync_channel(BUF_SIZE);
        Ok((
            Self {
                rx,
                store: TransactionStore::new()?,
                local_state: LocalState::default(),
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
            } => {
                self.local_state.deposit(client, value)?;
                Ok(self.store.insert(tx_id, tx)?)
            }
            // Withdrawal are not inserted in the tx store because they cannot be disputed
            Withdrawal { client, value, .. } => Ok(self.local_state.withdraw(client, value)?),
            Dispute { tx_id, client } => {
                let tx = self.store.fetch_dispute(tx_id)?;
                if client != tx.client() {
                    return Err(Error::MismatchedClient);
                }
                Ok(self
                    .local_state
                    .freeze_funds(client, tx.value().expect("invalid dispute tx"))?)
            }
            Chargeback { tx_id, client } => {
                let tx = self.store.fetch_resolve(tx_id)?;
                if client != tx.client() {
                    return Err(Error::MismatchedClient);
                }
                self.local_state
                    .chargeback(client, tx.value().expect("invalid dispute tx"))?;
                Ok(self.local_state.freeze_account(client)?)
            }
            Resolve { tx_id, client } => {
                let tx = self.store.fetch_resolve(tx_id)?;
                if client != tx.client() {
                    return Err(Error::MismatchedClient);
                }
                Ok(self
                    .local_state
                    .release_funds(client, tx.value().expect("invalid dispute tx"))?)
            }
        }
    }

    pub fn run(mut self) -> JoinHandle<LocalState> {
        std::thread::spawn(move || {
            // recv() will only fail on disconnection
            while let Ok(tx) = self.rx.recv() {
                // do not block on errors
                // transactions that result in errors will be ignored and will not put
                // the system in an invalid state
                if let Err(_e) = self.process_tx(tx) {
                    //eprintln!("error while processing tx {:?}: {}", tx, e); // some logging machinery would be better suited for this
                }
            }
            self.local_state
        })
    }
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("account not found")]
    AccountNotFound,
    #[error("account frozen")]
    AccountFrozen,
    #[error(transparent)]
    Account(#[from] account::AccountError),
}

/// View on a (sub)set of accounts
#[derive(Default, Debug, PartialEq)]
pub struct LocalState {
    // if there are a lot of clients this could be a tiered system
    // but it's fine as we only have u16::MAX accounts at most
    accounts: HashMap<Client, Account>,
}

impl LocalState {
    fn ensure_active_do<F>(
        &mut self,
        client: Client,
        create_on_miss: bool,
        f: F,
    ) -> Result<(), StateError>
    where
        F: FnOnce(&AccountInner<Active>) -> Result<Account, StateError>,
    {
        match self.accounts.entry(client) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                active @ Account::Active(_) => {
                    // a bit of mental gymnastic to please the borrow checker
                    let res = if let Account::Active(inner) = active {
                        f(inner)?
                    } else {
                        unreachable!();
                    };
                    *active = res;
                    Ok(())
                }
                Account::Frozen(_) => Err(StateError::AccountFrozen),
            },
            Entry::Vacant(entry) => {
                if create_on_miss {
                    entry.insert(f(&AccountInner::default())?);
                    Ok(())
                } else {
                    Err(StateError::AccountNotFound)
                }
            }
        }
    }

    fn deposit(&mut self, client: Client, amount: Value) -> Result<(), StateError> {
        self.ensure_active_do(client, true, |inner| {
            Ok(Account::Active(inner.deposit(amount)?))
        })
    }

    fn withdraw(&mut self, client: Client, amount: Value) -> Result<(), StateError> {
        self.ensure_active_do(client, false, |inner| {
            Ok(Account::Active(inner.withdraw(amount)?))
        })
    }

    fn chargeback(&mut self, client: Client, amount: Value) -> Result<(), StateError> {
        self.ensure_active_do(client, false, |inner| {
            Ok(Account::Active(inner.chargeback(amount)?))
        })
    }

    fn release_funds(&mut self, client: Client, amount: Value) -> Result<(), StateError> {
        self.ensure_active_do(client, false, |inner| {
            Ok(Account::Active(inner.release_funds(amount)?))
        })
    }

    pub fn freeze_funds(&mut self, client: Client, amount: Value) -> Result<(), StateError> {
        self.ensure_active_do(client, false, |inner| {
            Ok(Account::Active(inner.freeze_funds(amount)?))
        })
    }

    fn freeze_account(&mut self, client: Client) -> Result<(), StateError> {
        self.ensure_active_do(client, false, |inner| Ok(Account::Frozen(inner.freeze())))
    }

    /// Merge two local states together. An account should only be present in one local state
    pub fn merge(mut self, other: Self) -> Self {
        self.accounts.extend(other.accounts);
        self
    }
}

impl IntoIterator for LocalState {
    type Item = (Client, Account);
    type IntoIter = hashbrown::hash_map::IntoIter<Client, Account>;
    fn into_iter(self) -> Self::IntoIter {
        self.accounts.into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::*;
    const CLIENT: u16 = 0;

    #[quickcheck]
    fn test_deposit(tx: Transaction) -> TestResult {
        if let Transaction::Deposit { client, value, .. } = tx {
            TestResult::from_bool(
                Engine::new(1)
                    .unwrap()
                    .run([tx].into_iter())
                    .unwrap()
                    .accounts
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
                    .accounts
                    .get(&client)
                    .is_none(),
            )
        } else {
            TestResult::discard()
        }
    }

    #[test]
    fn test_deposit_withdraw() {
        let mut state = LocalState::default();
        state.deposit(CLIENT, Value::TEN).unwrap();
        assert_eq!(state.accounts.get(&CLIENT).unwrap().available(), Value::TEN);
        state.withdraw(CLIENT, Value::ONE).unwrap();
        assert_eq!(
            state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN - Value::ONE
        );
    }

    #[test]
    fn test_freeze_release() {
        let mut state = LocalState::default();
        state.deposit(CLIENT, Value::TEN).unwrap();
        state.freeze_funds(CLIENT, Value::ONE).unwrap();
        assert_eq!(
            state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN - Value::ONE
        );
        assert_eq!(state.accounts.get(&CLIENT).unwrap().held(), Value::ONE);
        state.release_funds(CLIENT, Value::ONE).unwrap();
        assert_eq!(state.accounts.get(&CLIENT).unwrap().available(), Value::TEN);
        assert_eq!(state.accounts.get(&CLIENT).unwrap().held(), Value::ZERO);
    }

    #[test]
    fn test_freeze_chargeback() {
        let mut state = LocalState::default();
        state.deposit(CLIENT, Value::TEN).unwrap();
        state.freeze_funds(CLIENT, Value::ONE).unwrap();
        assert_eq!(
            state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN - Value::ONE
        );
        assert_eq!(state.accounts.get(&CLIENT).unwrap().held(), Value::ONE);
        state.chargeback(CLIENT, Value::ONE).unwrap();
        assert_eq!(
            state.accounts.get(&CLIENT).unwrap().available(),
            Value::TEN - Value::ONE
        );
        assert_eq!(state.accounts.get(&CLIENT).unwrap().held(), Value::ZERO);
    }

    #[test]
    fn test_locked() {
        let mut state = LocalState::default();
        state.deposit(CLIENT, Value::ONE).unwrap();
        state.freeze_account(CLIENT).unwrap();
        let account = state.accounts.get(&CLIENT).unwrap();
        match account {
            Account::Active(_) => panic!("account should be frozen"),
            Account::Frozen(a) => {
                assert_eq!(a.available, Value::ONE);
                assert_eq!(a.held, Value::ZERO);
            }
        }
        assert!(state.deposit(CLIENT, Value::ONE).is_err());
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
