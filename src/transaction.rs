use super::common::*;
use ::serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Transaction {
    Deposit {
        client: Client,
        tx_id: TxId,
        #[serde(with = "rust_decimal::serde::str")]
        value: Value,
    },
    Withdrawal {
        client: Client,
        tx_id: TxId,
        #[serde(with = "rust_decimal::serde::str")]
        value: Value,
    },
    Dispute {
        tx_id: TxId,
        client: Client,
    },
    Resolve {
        tx_id: TxId,
        client: Client,
    },
    Chargeback {
        tx_id: TxId,
        client: Client,
    },
}

impl Transaction {
    pub fn client(&self) -> Client {
        match self {
            Self::Deposit { client, .. }
            | Self::Withdrawal { client, .. }
            | Self::Dispute { client, .. }
            | Self::Resolve { client, .. }
            | Self::Chargeback { client, .. } => *client,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
pub enum TxStatus {
    Undisputed = 0,
    Disputed = 1,
    // In theory there are more possible status for transactions: Undisputed, Disputed and Resolved/ChargedBack, but assuming
    // transactions cannot be disputed more than once, we can remove already resolved transactions from the db
    // since we're not going to need them anymore
}

// A bit annoying, internally tagged enums do not work with csv.
// Since the enum allow for conserving invariants at compile time (e.g. deposit must have an amount, resolve will not)
// over the natural serde-compatible translation (TransactionCompat), add custom deserialization instead of changing
// our type.
pub mod serde {
    use super::*;
    #[derive(Deserialize, Debug)]
    pub struct TransactionCompatCsv {
        #[serde(rename = "type")]
        kind: TType,
        client: Client,
        tx: TxId,
        #[serde(alias = "value")] // TODO: remove
        amount: Option<Value>,
    }
    #[derive(Deserialize, Debug, Copy, Clone)]
    #[serde(rename_all = "lowercase")]
    enum TType {
        Deposit,
        Withdrawal,
        Dispute,
        Resolve,
        Chargeback,
    }

    impl TryFrom<TransactionCompatCsv> for Transaction {
        type Error = std::io::Error;
        fn try_from(tx: TransactionCompatCsv) -> Result<Self, Self::Error> {
            match (tx.kind, tx.amount) {
                (TType::Deposit, Some(value)) if value >= Value::ZERO => Ok(Self::Deposit {
                    client: tx.client,
                    tx_id: tx.tx,
                    value,
                }),
                (TType::Withdrawal, Some(value)) if value >= Value::ZERO => Ok(Self::Withdrawal {
                    client: tx.client,
                    tx_id: tx.tx,
                    value,
                }),
                (TType::Dispute, None) => Ok(Self::Dispute {
                    client: tx.client,
                    tx_id: tx.tx,
                }),
                (TType::Resolve, None) => Ok(Self::Resolve {
                    client: tx.client,
                    tx_id: tx.tx,
                }),
                (TType::Chargeback, None) => Ok(Self::Chargeback {
                    client: tx.client,
                    tx_id: tx.tx,
                }),
                // a little more work should be put in this error report
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid data: {tx:?}"),
                )),
            }
        }
    }
}

#[doc(hidden)]
#[cfg(test)]
pub mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};

    impl Arbitrary for Transaction {
        fn arbitrary(g: &mut Gen) -> Self {
            match u32::arbitrary(g) % 5 {
                0 => Self::Deposit {
                    client: u16::arbitrary(g),
                    value: Value::new(i64::arbitrary(g), u32::arbitrary(g) % 28),
                    tx_id: u32::arbitrary(g),
                },
                1 => Self::Withdrawal {
                    client: u16::arbitrary(g),
                    value: Value::new(i64::arbitrary(g), u32::arbitrary(g) % 28),
                    tx_id: u32::arbitrary(g),
                },
                2 => Self::Dispute {
                    client: u16::arbitrary(g),
                    tx_id: u32::arbitrary(g),
                },
                3 => Self::Resolve {
                    client: u16::arbitrary(g),
                    tx_id: u32::arbitrary(g),
                },
                4 => Self::Chargeback {
                    client: u16::arbitrary(g),
                    tx_id: u32::arbitrary(g),
                },
                _ => unreachable!(),
            }
        }
    }
}
