use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
};

type WorkList = VecDeque<Option<Box<dyn FnOnce() + Send + Sync + 'static>>>;
type WorksPair = Arc<(Mutex<WorkList>, Condvar)>;

struct Worker {
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(pair: WorksPair) -> Self{
        let handle = thread::spawn(move || {
            let (tasks, cvar) = &*pair;
            let mut tasks = tasks.lock().unwrap();
            loop {
                tasks = cvar.wait_while(tasks, |tasks| tasks.is_empty()).unwrap();
                let work = tasks.pop_front().unwrap();
                if let Some(work) = work {
                    work();
                } else {
                    break;
                }
            }
        });
        Self {
            handle: Some(handle),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            _ = handle.join();
        }
    }
}

pub struct ThreadPool{
    workers: Vec<(Worker, WorksPair)>,
    round: usize,
}

impl ThreadPool{
    pub fn new(size: usize) -> Self{
        let mut v = Vec::with_capacity(size);
        for _ in 0..size{
            let workspair = Arc::new((Mutex::new(WorkList::new()), Condvar::new()));
            let wp_clone = Arc::clone(&workspair);
            v.push((Worker::new(workspair), wp_clone));
        }
        Self{
            workers: v,
            round: 0,
        }
    }

    pub fn spawn<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + Sync + 'static
    {
        self.round %= self.workers.len();
        self.round += 1;
        let (_, pair) = &self.workers[self.round];
        let (ref tasks, ref cvar) = **pair;
        let mut tasks = tasks.lock().unwrap();
        tasks.push_back(Some(Box::new(f)));
        cvar.notify_one();
    }
}


impl Drop for ThreadPool{
    fn drop(&mut self){
        for (_, pair) in &mut self.workers{
            let (ref tasks, ref cvar) = **pair;
            let mut tasks = tasks.lock().unwrap();
            tasks.push_back(None);
            cvar.notify_one();
        }
    }
}
