use std::{
    sync::mpsc,
    mem::ManuallyDrop,
    sync::{
        mpsc::{Sender, Receiver,},
    },
    thread::{self, JoinHandle},
    time::Instant,
};


type Task = Box<dyn FnOnce() + Send + Sync + 'static>;

struct Worker {
    handle: ManuallyDrop<JoinHandle<()>>,
    compara
}

impl Worker {
    fn new(tasks: Receiver<Task>) -> Self{
        let handle = thread::spawn(move || {
            for task in tasks{
                task();
            }
        });
        Self {
            handle: ManuallyDrop::new(handle),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe{
            let handle = ManuallyDrop::take(&mut self.handle);
            _ = handle.join();
        }
    }
}

pub struct ThreadPool{
    workers: Vec<(Worker, ManuallyDrop<Sender<Task>>)>,
    round: usize,
}

impl ThreadPool{
    pub fn new(size: usize) -> Self{
        let mut v = Vec::with_capacity(size);
        for _ in 0..size{
            let (tx, rx) = mpsc::channel();
            v.push((Worker::new(rx), ManuallyDrop::new(tx)));
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
        _ = self.workers[self.round].1.send(Box::new(f));
        self.round += 1;
    }
}


impl Drop for ThreadPool{
    fn drop(&mut self){
        for (_, tx) in &mut self.workers{
            unsafe{
                _ = ManuallyDrop::take(tx);
            }
        }
    }
}
