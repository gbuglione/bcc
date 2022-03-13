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

    pub fn value(&self) -> Option<Value> {
        match self {
            Self::Deposit { value, .. } | Self::Withdrawal { value, .. } => Some(*value),
            _ => None,
        }
    }
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
                (TType::Deposit, Some(value)) if value >= Value::ZERO => Ok(Transaction::Deposit {
                    client: tx.client,
                    tx_id: tx.tx,
                    value,
                }),
                (TType::Withdrawal, Some(value)) if value >= Value::ZERO => {
                    Ok(Transaction::Withdrawal {
                        client: tx.client,
                        tx_id: tx.tx,
                        value,
                    })
                }
                (TType::Dispute, None) => Ok(Transaction::Dispute {
                    client: tx.client,
                    tx_id: tx.tx,
                }),
                (TType::Resolve, None) => Ok(Transaction::Resolve {
                    client: tx.client,
                    tx_id: tx.tx,
                }),
                (TType::Chargeback, None) => Ok(Transaction::Chargeback {
                    client: tx.client,
                    tx_id: tx.tx,
                }),
                // a little more work should be put in this error report
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid data: {:?}", tx),
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
    use quickcheck_macros::*;

    impl Arbitrary for Transaction {
        fn arbitrary(g: &mut Gen) -> Self {
            match u32::arbitrary(g) % 5 {
                0 => Transaction::Deposit {
                    client: u16::arbitrary(g),
                    value: Value::new(i64::arbitrary(g), u32::arbitrary(g) % 28),
                    tx_id: u32::arbitrary(g),
                },
                1 => Transaction::Withdrawal {
                    client: u16::arbitrary(g),
                    value: Value::new(i64::arbitrary(g), u32::arbitrary(g) % 28),
                    tx_id: u32::arbitrary(g),
                },
                2 => Transaction::Dispute {
                    client: u16::arbitrary(g),
                    tx_id: u32::arbitrary(g),
                },
                3 => Transaction::Resolve {
                    client: u16::arbitrary(g),
                    tx_id: u32::arbitrary(g),
                },
                4 => Transaction::Chargeback {
                    client: u16::arbitrary(g),
                    tx_id: u32::arbitrary(g),
                },
                _ => unreachable!(),
            }
        }
    }

    #[quickcheck]
    fn test_serde(tx: Transaction) {
        assert_eq!(
            tx,
            bincode::deserialize::<Transaction>(&bincode::serialize(&tx).unwrap()).unwrap()
        );
    }
}
