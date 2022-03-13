use bcc::account::Account;
use bcc::common::*;
use bcc::engine::{self, Accounts};
use bcc::transaction::{serde::TransactionCompatCsv, Transaction};
use clap::Parser;
use std::path::PathBuf;
use thiserror::Error;

/// Assumptions made in the assignment:
/// * dispute, resolve and chargeback all reference transactions by the same client
/// * only deposits can be disputed. This is a bit unclear for me, but the actions described
///   in the doc for dispute seemed only appliable for deposits (same for resolve and chargeback)
/// * deposit and withdrawal amounts are non negative

#[derive(Parser)]
struct Cmd {
    /// Input file for transactions
    path: PathBuf,
    /// Output file for accounts, defaults to stdio
    output_file: Option<PathBuf>,
}

#[derive(Debug, Error)]
enum Error {
    #[error(transparent)]
    Engine(#[from] engine::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

impl Cmd {
    // This is sync for now since we only have to read from one file but can be turned into async rather easily
    fn exec(self) -> Result<(), Error> {
        let records = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .from_path(self.path)?
            .into_deserialize::<TransactionCompatCsv>()
            .map(|maybe_tx| Ok::<_, Error>(Transaction::try_from(maybe_tx?)?));

        let mut engine = engine::Engine::new(num_cpus::get())?;
        for tx in records {
            engine.feed(tx?)?;
        }

        let state = engine.finish()?;
        if let Some(filepath) = self.output_file {
            Ok(write_state_to_csv(
                state,
                std::fs::File::create(&filepath)?,
            )?)
        } else {
            Ok(write_state_to_csv(state, std::io::stdout())?)
        }
    }
}

fn main() -> Result<(), Error> {
    Cmd::parse().exec()
}

fn write_state_to_csv<W: std::io::Write>(accounts: Accounts, writer: W) -> std::io::Result<()> {
    #[derive(serde::Serialize)]
    struct Record {
        client: Client,
        available: Value,
        held: Value,
        total: Value,
        locked: bool,
    }

    impl From<(Client, Account)> for Record {
        fn from(from: (Client, Account)) -> Record {
            match from {
                (client, Account::Active(inner)) => Record {
                    client,
                    available: inner.available,
                    held: inner.held,
                    total: inner.available + inner.held,
                    locked: false,
                },
                (client, Account::Frozen(inner)) => Record {
                    client,
                    available: inner.available,
                    held: inner.held,
                    total: inner.available + inner.held,
                    locked: true,
                },
            }
        }
    }

    let mut writer = csv::Writer::from_writer(writer);
    for record in accounts.into_iter() {
        writer.serialize(Record::from(record))?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;

    #[test]
    fn example() {
        let csv = r#"
    type, client, tx, amount
    deposit, 1, 1, 1.0
    deposit, 2, 2, 2.0
    deposit, 1, 3, 2.0
    withdrawal, 1, 4, 1.5
    withdrawal, 2, 5, 3.0
"#;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let out = tempfile::NamedTempFile::new().unwrap();
        file.write_all(csv.as_bytes()).unwrap();

        Cmd {
            path: file.path().to_path_buf(),
            output_file: Some(out.path().to_owned()),
        }
        .exec()
        .unwrap();

        let mut found = std::fs::read_to_string(out.path())
            .unwrap()
            .replace(" ", "")
            .split('\n')
            .map(String::from)
            .collect::<Vec<_>>();
        found[1..3].sort();
        assert_eq!(
            found[0..3],
            r#"client,available,held,total,locked
            1,1.5,0,1.5,false
            2,2.0,0,2.0,false"#
                .replace(" ", "")
                .split('\n')
                .collect::<Vec<_>>()
        );
    }
}
