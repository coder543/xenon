extern crate xenon;

extern crate env_logger;
extern crate log;

use std::thread;
use std::time::Duration;

use xenon::WorkerPool;

fn main() {
    env_logger::init();

    // minimum of 3 workers in the pool when idle
    // maximum of 64 workers
    // "hello" is a `Clone` context variable, which could be an r2d2 connection pool
    // the function provided operates on a .clone() of the context and the next element, returning whatever it wants
    let pool = WorkerPool::new(3, 64, "hello", |_ctx, elem| {
        // println!("ctx: {}, elem: {:?}", ctx, elem);
        thread::sleep(Duration::from_millis(50));
        elem
    });

    let mut results = vec![];
    for i in 0..=10000 {
        // each call to pool.submit returns a Receiver channel that will
        // eventually hold the result value of the function
        results.push(pool.submit(i));
    }

    println!(
        "total: {}",
        results.into_iter().map(|v| v.recv().unwrap()).sum::<u64>()
    );
}
