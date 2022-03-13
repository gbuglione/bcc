use bcc::common::*;
use bcc::transaction::Transaction;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaChaRng;

fn gen_inputs(size: usize) -> Vec<Transaction> {
    let mut rng = ChaChaRng::from_seed([0; 32]);
    let mut res = Vec::new();
    for i in 0..size {
        let tx = match rng.next_u32() % 8 {
            0..=2 => Transaction::Deposit {
                client: rng.gen::<u16>(),
                value: Value::new(rng.gen::<i64>(), rng.next_u32() % 28),
                tx_id: i as u32,
            },
            3..=4 => Transaction::Withdrawal {
                client: rng.gen::<u16>(),
                value: Value::new(rng.gen::<i64>(), rng.next_u32() % 28),
                tx_id: i as u32,
            },
            5 => Transaction::Dispute {
                client: rng.gen::<u16>(),
                tx_id: rng.gen::<u32>() % (i + 1) as u32,
            },
            6 => Transaction::Resolve {
                client: rng.gen::<u16>(),
                tx_id: rng.gen::<u32>() % (i + 1) as u32,
            },
            7 => Transaction::Chargeback {
                client: rng.gen::<u16>(),
                tx_id: rng.gen::<u32>() % (i + 1) as u32,
            },
            _ => unreachable!(),
        };
        res.push(tx);
    }
    res
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Exec");
    let input_size = [1000, 10_000, 100_000, 1_000_000];
    for i in input_size.iter() {
        group.bench_with_input(BenchmarkId::new("1 core", i), i, |b, i| {
            let input = gen_inputs(*i);
            b.iter(|| {
                bcc::engine::Engine::new(1)
                    .unwrap()
                    .run(input.clone().into_iter())
            })
        });
        group.bench_with_input(BenchmarkId::new("2 core", i), i, |b, i| {
            let input = gen_inputs(*i);
            b.iter(|| {
                bcc::engine::Engine::new(2)
                    .unwrap()
                    .run(input.clone().into_iter())
            })
        });
        group.bench_with_input(BenchmarkId::new("4 core", i), i, |b, i| {
            let input = gen_inputs(*i);
            b.iter(|| {
                bcc::engine::Engine::new(4)
                    .unwrap()
                    .run(input.clone().into_iter())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
