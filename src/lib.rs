#[macro_use]
extern crate crossbeam_channel as channel;

#[macro_use]
extern crate log;

use std::cmp;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use channel::{Receiver, Sender};

// maximum elements in the pool queue
const POOL_QUEUE_CAP: usize = 10;

// if there are more than this many elements
// in the queue, then it's time to spawn a new worker
const SUPERVISOR_THRESHOLD: usize = 5;

// how long the supervisor waits before each loop
// to check if a new worker needs to be spawned
const SUPERVISOR_LOOP_DELAY: u64 = 5;

// maximum milliseconds of worker idle before worker exits
const WORKER_MAX_IDLE: u64 = 500;

pub struct WorkerPool<Ctx, Lambda, Element, Res>
where
    Ctx: Clone + Send + Sync + 'static,
    Lambda: Fn(Ctx, Element) -> Res + Clone + Send + 'static,
    Element: Send + Sync + 'static,
    Res: Send + Sync + 'static,
{
    context: Ctx,
    sender: Sender<(Element, Sender<Res>)>,
    receiver: Receiver<(Element, Sender<Res>)>,
    worker_func: Lambda,
    num_workers: Arc<AtomicUsize>,
    min_workers: usize,
    max_workers: Option<usize>,
}

// derive(Clone) doesn't work because some parameters are not Clone,
// even though those parameters aren't used as members
impl<Ctx, Lambda, Element, Res> Clone for WorkerPool<Ctx, Lambda, Element, Res>
where
    Ctx: Clone + Send + Sync + 'static,
    Lambda: Fn(Ctx, Element) -> Res + Clone + Send + 'static,
    Element: Send + Sync + 'static,
    Res: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        WorkerPool {
            context: self.context.clone(),
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            worker_func: self.worker_func.clone(),
            num_workers: self.num_workers.clone(),
            min_workers: self.min_workers,
            max_workers: self.max_workers,
        }
    }
}

impl<Ctx, Lambda, Element, Res> WorkerPool<Ctx, Lambda, Element, Res>
where
    Ctx: Clone + Send + Sync + 'static,
    Lambda: Fn(Ctx, Element) -> Res + Clone + Send + 'static,
    Element: Send + Sync + 'static,
    Res: Send + Sync + 'static,
{
    pub fn new(
        min_workers: impl Into<Option<usize>>,
        max_workers: impl Into<Option<usize>>,
        context: Ctx,
        worker_func: Lambda,
    ) -> WorkerPool<Ctx, Lambda, Element, Res> {
        let min_workers = min_workers.into();
        let max_workers = max_workers.into();

        let (sender, receiver) = channel::bounded(POOL_QUEUE_CAP);
        let num_workers = Arc::new(AtomicUsize::new(0));
        let min_workers = cmp::max(1, min_workers.unwrap_or(1));

        let pool = WorkerPool {
            context,
            sender,
            receiver,
            worker_func,
            num_workers,
            min_workers,
            max_workers,
        };

        let min_workers = pool.min_workers;
        let max_workers = pool.max_workers;
        let num_workers = pool.num_workers.clone();
        let receiver = pool.receiver.clone();
        let worker_func = pool.worker_func.clone();
        let context = pool.context.clone();
        //launch the supervisor task
        thread::spawn(move || {
            // spawn the minimum workers
            for _ in 0..min_workers {
                WorkerPool::spawn(min_workers, &num_workers, &context, &receiver, &worker_func);
            }
            loop {
                let cur_workers = num_workers.load(Ordering::Relaxed);

                if cur_workers == 0 {
                    break;
                }

                let room_in_pool = max_workers
                    .map(|max_workers| cur_workers < max_workers)
                    .unwrap_or(true);

                if room_in_pool && receiver.len() > SUPERVISOR_THRESHOLD {
                    WorkerPool::spawn(min_workers, &num_workers, &context, &receiver, &worker_func);
                }

                thread::sleep(Duration::from_millis(SUPERVISOR_LOOP_DELAY));
            }
            debug!("xenon supervisor exiting")
        });

        pool
    }

    fn spawn(
        min_workers: usize,
        num_workers: &Arc<AtomicUsize>,
        context: &Ctx,
        receiver: &Receiver<(Element, Sender<Res>)>,
        worker_func: &Lambda,
    ) {
        let num_workers = num_workers.clone();
        let thread_num = num_workers.fetch_add(1, Ordering::SeqCst);
        debug!("xenon worker spawned, {} workers", thread_num + 1);
        let receiver = receiver.clone();
        let context = context.clone();
        let worker_func = worker_func.clone();
        thread::spawn(move || {
            loop {
                select! {
                    recv(receiver, msg) => match msg {
                        Some((elem, sender)) => {
                            let context = context.clone();
                            let res = worker_func(context, elem);
                            sender.send(res);
                        },
                        None => {
                            // sender has been dropped
                            let cur_workers = num_workers.fetch_sub(1, Ordering::SeqCst);
                            debug!("end of xenon worker pool, {} left.", cur_workers - 1);
                            break;
                        }
                    },
                    recv(channel::after(Duration::from_millis(WORKER_MAX_IDLE))) => {
                        let cur_workers = num_workers.load(Ordering::SeqCst);
                        // we always want to keep min_workers in the pool unless the pool is dropped
                        if cur_workers > min_workers  {
                            // only quit if we successfully updated the atomic
                            if cur_workers == num_workers.compare_and_swap(cur_workers, cur_workers - 1, Ordering::SeqCst) {
                                debug!("idle xenon worker quitting, {} left", cur_workers - 1);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    pub fn submit(&self, element: Element) -> Receiver<Res> {
        let (sender, receiver) = channel::bounded(1);
        self.sender.send((element, sender));
        receiver
    }
}
