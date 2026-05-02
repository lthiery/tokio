use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::{
    runtime::Runtime,
    time::{sleep, timeout},
};

// a very quick async task, but might timeout
async fn quick_job() -> usize {
    1
}

fn build_run_time(workers: usize) -> Runtime {
    if workers == 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.enable_all().worker_threads(workers);
        // Bench-mode A/B knobs. Independent since the rt-alt-timer feature
        // gate landed in Step 3: enabling sharded-mio no longer implies the
        // alt-timer flavor. Combinations:
        //   bench-alt-timer alone           → traditional I/O + alt-timer
        //   bench-sharded-mio alone         → sharded-mio I/O + legacy timer
        //                                     (hybrid park flow)
        //   both                            → sharded-mio I/O + alt-timer
        #[cfg(all(tokio_unstable, feature = "bench-alt-timer"))]
        b.enable_alt_timer();
        #[cfg(all(tokio_unstable, feature = "bench-sharded-mio", target_os = "linux"))]
        b.enable_sharded_mio();
        b.build().unwrap()
    }
}

fn single_thread_scheduler_timeout(c: &mut Criterion) {
    do_timeout_test(c, 1, "single_thread_timeout");
}

fn multi_thread_scheduler_timeout(c: &mut Criterion) {
    do_timeout_test(c, 8, "multi_thread_timeout-8");
}

fn do_timeout_test(c: &mut Criterion, workers: usize, name: &str) {
    let runtime = build_run_time(workers);
    c.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                black_box(spawn_timeout_job(iters as usize, workers)).await;
            });
            start.elapsed()
        })
    });
}

async fn spawn_timeout_job(iters: usize, procs: usize) {
    let mut handles = Vec::with_capacity(procs);
    for _ in 0..procs {
        handles.push(tokio::spawn(async move {
            for _ in 0..iters / procs {
                let h = timeout(Duration::from_secs(1), quick_job());
                assert_eq!(black_box(h.await.unwrap()), 1);
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

fn single_thread_scheduler_sleep(c: &mut Criterion) {
    do_sleep_test(c, 1, "single_thread_sleep");
}

fn multi_thread_scheduler_sleep(c: &mut Criterion) {
    do_sleep_test(c, 8, "multi_thread_sleep-8");
}

fn do_sleep_test(c: &mut Criterion, workers: usize, name: &str) {
    let runtime = build_run_time(workers);

    c.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                black_box(spawn_sleep_job(iters as usize, workers)).await;
            });
            start.elapsed()
        })
    });
}

async fn spawn_sleep_job(iters: usize, procs: usize) {
    let mut handles = Vec::with_capacity(procs);
    for _ in 0..procs {
        handles.push(tokio::spawn(async move {
            for _ in 0..iters / procs {
                let _h = black_box(sleep(Duration::from_secs(1)));
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

criterion_group!(
    timeout_benchmark,
    single_thread_scheduler_timeout,
    multi_thread_scheduler_timeout,
    single_thread_scheduler_sleep,
    multi_thread_scheduler_sleep
);

criterion_main!(timeout_benchmark);
