/*
Restrictions:
- Cannot delete consumer erratically after adding - can only decrement from latest added consumer
- Can talk about Relax -> Acquire ordering on line 208
- Can talk about loom + mrsi testing
- Want to build
    - naive lock implementation: compare w this
    - implementation without CacheAligned - check memory thruput
    - implementation without caching of pointer in producer
- ONLY key thing to take note in this implementation is ordering of acquiring of lock - head -> tail (never invert, and never deadlock)
*/

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::usize::MAX;

pub struct SyncRingBuffer<T, const CAPACITY: usize, const NUM_CONSUMERS: usize> {
    heads: Arc<[Arc<RwLock<usize>>; NUM_CONSUMERS]>,
    tail: Arc<RwLock<usize>>,
    num_producers: Arc<AtomicUsize>,
    num_consumers: Arc<AtomicUsize>,

    // Unpadded read-only fields packed together
    // UnsafeCell -> allow for mutation of elements across threads - without self being mutable
    // reference + data directly in struct
    // buffer: UnsafeCell<[Option<T>; CAPACITY]>,
    buffer: Arc<[RwLock<Option<T>>; CAPACITY]>,
}

impl<T: Clone, const CAPACITY: usize, const NUM_CONSUMERS: usize>
    SyncRingBuffer<T, CAPACITY, NUM_CONSUMERS>
{
    const CHECK: () = assert!(
        CAPACITY.is_power_of_two(),
        "SyncRingBuffer CAPACITY must be a power of two"
    );

    pub fn new() -> Self {
        let () = Self::CHECK;
        Self {
            heads: Arc::new(std::array::from_fn(|_| Arc::new(RwLock::new(0)))),
            tail: Arc::new(RwLock::new(0)),
            num_producers: Arc::new(AtomicUsize::new(0)),
            num_consumers: Arc::new(AtomicUsize::new(0)),
            buffer: Arc::new([const { RwLock::new(None) }; CAPACITY]),
        }
    }

    pub fn get_new_producer(&self) -> Option<SyncRingBufferProducer<T, CAPACITY, NUM_CONSUMERS>> {
        if let Err(_) = self.num_producers.compare_exchange(
            0,
            1,
            Ordering::Acquire,
            Ordering::Relaxed, // check if Acquire here makes a diff
        ) {
            return None;
        }

        Some(SyncRingBufferProducer {
            heads: Arc::clone(&self.heads),
            tail: Arc::clone(&self.tail),
            buffer: Arc::clone(&self.buffer),
            num_consumers: Arc::clone(&self.num_consumers),
        })
    }

    pub fn get_new_consumer(&self) -> Option<SyncRingBufferConsumer<T, CAPACITY>> {
        let mut current_consumers = self.num_consumers.load(Ordering::Acquire);
        loop {
            // 1. Guard check: Reject if maximum consumers reached
            if current_consumers >= NUM_CONSUMERS {
                return None;
            }

            // 2. Atomic CAS to reserve a consumer slot safely
            match self.num_consumers.compare_exchange_weak(
                current_consumers,
                current_consumers + 1,
                Ordering::Release,
                Ordering::Relaxed, // check if Acquire here makes a diff
            ) {
                Ok(_) => break,
                Err(actual) => current_consumers = actual, // Retry with updated count
            }
        }

        let head = Arc::clone(&self.heads[current_consumers]);
        Some(SyncRingBufferConsumer {
            head,
            tail: Arc::clone(&self.tail),
            buffer: Arc::clone(&self.buffer),
        })
    }
}

pub struct SyncRingBufferProducer<T, const CAPACITY: usize, const NUM_CONSUMERS: usize> {
    heads: Arc<[Arc<RwLock<usize>>; NUM_CONSUMERS]>,
    tail: Arc<RwLock<usize>>,
    buffer: Arc<[RwLock<Option<T>>; CAPACITY]>,

    num_consumers: Arc<AtomicUsize>,
}

impl<T: Clone, const CAPACITY: usize, const NUM_CONSUMERS: usize>
    SyncRingBufferProducer<T, CAPACITY, NUM_CONSUMERS>
{
    pub fn try_push(&self, item: T) -> Result<(), T> {
        // get earliest head - cached, and if cached is full, then check updated values
        let mut slowest_head = MAX;
        for i in 0..self.num_consumers.load(Ordering::SeqCst) {
            slowest_head = slowest_head.min(*self.heads[i].read().map_err(|_| item.clone())?);
        }

        let mut tail = self.tail.write().map_err(|_| item.clone())?;
        if *tail - slowest_head == CAPACITY {
            return Err(item.clone());
        }

        // write data
        let mut buffer_writer = self.buffer[*tail & (CAPACITY - 1)]
            .write()
            .map_err(|_| item.clone())?;
        buffer_writer.replace(item.clone());

        // update tail pointer
        *tail = tail.strict_add(1);

        Ok(())
    }

    pub fn push(&self, item: T) {
        let mut item = item;
        let mut backoff_num = 1;
        loop {
            match self.try_push(item) {
                Ok(()) => return,
                Err(returned) => {
                    item = returned;
                    for _ in 0..backoff_num {
                        std::hint::spin_loop();
                    }
                    backoff_num += 1;
                }
            }
        }
    }
}

pub struct SyncRingBufferConsumer<T, const CAPACITY: usize> {
    head: Arc<RwLock<usize>>,
    tail: Arc<RwLock<usize>>,
    buffer: Arc<[RwLock<Option<T>>; CAPACITY]>,
}

impl<T: Clone, const CAPACITY: usize> SyncRingBufferConsumer<T, CAPACITY> {
    pub fn try_pop(&self) -> Option<T> {
        let mut head = self.head.write().ok()?;
        let val = {
            let tail = self.tail.read().ok()?;

            if *tail - *head == 0 {
                // ring buffer is empty
                return None;
            }

            // read data
            self.buffer[*head & (CAPACITY - 1)]
                .read()
                .ok()?
                .as_ref()
                .unwrap()
                .clone()
        };

        // update head pointer - syncs with previous read statement
        *head = head.strict_add(1);

        Some(val)
    }

    pub fn pop(&self) -> T {
        let mut backoff_num = 1;
        loop {
            match self.try_pop() {
                Some(item) => return item,
                None => {
                    for _ in 0..backoff_num {
                        std::hint::spin_loop();
                    }
                    backoff_num += 1;
                }
            }
        }
    }
}
