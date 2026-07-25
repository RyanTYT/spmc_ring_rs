// Restrictions:
// - Cannot delete consumer erratically after adding - can only decrement from latest added consumer

use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::usize::MAX;

// const SPMC_RING_SIZE: usize = 2_usize.pow(3);
// const NUM_CONSUMERS: usize = 4_usize;

// Align to 64 bytes to protect against 64B prefetching & 64B cache lines - on i7
#[repr(align(64))]
pub struct CacheAligned<T>(pub T);

impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Manual Sync/Send implementation since we use UnsafeCell
unsafe impl<T: Send, const CAPACITY: usize, const NUM_CONSUMERS: usize> Sync
    for SpmcRingBuffer<T, CAPACITY, NUM_CONSUMERS>
{
}
unsafe impl<T: Send, const CAPACITY: usize> Sync for SpmcRingBufferProducer<T, CAPACITY> {}
unsafe impl<T: Send, const CAPACITY: usize> Sync for SpmcRingBufferConsumer<T, CAPACITY> {}
unsafe impl<T: Send, const CAPACITY: usize, const NUM_CONSUMERS: usize> Send
    for SpmcRingBuffer<T, CAPACITY, NUM_CONSUMERS>
{
}
unsafe impl<T: Send, const CAPACITY: usize> Send for SpmcRingBufferProducer<T, CAPACITY> {}
unsafe impl<T: Send, const CAPACITY: usize> Send for SpmcRingBufferConsumer<T, CAPACITY> {}

pub struct SpmcRingBuffer<T, const CAPACITY: usize, const NUM_CONSUMERS: usize> {
    heads: UnsafeCell<[CacheAligned<AtomicUsize>; NUM_CONSUMERS]>,
    tail: CacheAligned<AtomicUsize>,
    num_producers: AtomicUsize,
    num_consumers: AtomicUsize,

    // Unpadded read-only fields packed together
    // UnsafeCell -> allow for mutation of elements across threads - without self being mutable
    // reference + data directly in struct
    buffer: UnsafeCell<[Option<T>; CAPACITY]>,
}

impl<T: Clone, const CAPACITY: usize, const NUM_CONSUMERS: usize>
    SpmcRingBuffer<T, CAPACITY, NUM_CONSUMERS>
{
    const CHECK: () = assert!(
        CAPACITY.is_power_of_two(),
        "SpmcRingBuffer CAPACITY must be a power of two"
    );

    pub fn new() -> Self {
        let () = Self::CHECK;
        Self {
            // head: CacheAligned(AtomicUsize::new(0)),
            heads: UnsafeCell::new([const { CacheAligned(AtomicUsize::new(0)) }; NUM_CONSUMERS]),
            tail: CacheAligned(AtomicUsize::new(0)),
            num_producers: AtomicUsize::new(0),
            num_consumers: AtomicUsize::new(0),
            buffer: UnsafeCell::new([const { None }; CAPACITY]),
        }
    }

    pub fn get_new_producer(&self) -> Option<SpmcRingBufferProducer<T, CAPACITY>> {
        if let Err(_) = self.num_producers.compare_exchange_weak(
            0,
            1,
            Ordering::Release,
            Ordering::Relaxed, // check if Acquire here makes a diff
        ) {
            return None;
        };

        let heads = unsafe {
            let buffer_ptr = self.heads.get();
            (*buffer_ptr).as_mut_ptr()
        };
        let tail = &self.tail.0 as *const AtomicUsize;
        let buffer = unsafe {
            let buffer_ptr = self.buffer.get();
            (*buffer_ptr).as_mut_ptr()
        };
        let last_slowest_head = UnsafeCell::new(0_usize);
        let num_consumers = &self.num_consumers as *const AtomicUsize;
        Some(SpmcRingBufferProducer {
            last_slowest_head,
            heads,
            tail,
            buffer,

            num_consumers,
        })
    }

    pub fn get_new_consumer(&self) -> Option<SpmcRingBufferConsumer<T, CAPACITY>> {
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

        let head = unsafe {
            let head_ptr = self.heads.get();
            (*head_ptr).as_mut_ptr().add(current_consumers)
        };
        let tail = &self.tail.0 as *const AtomicUsize;
        let buffer = unsafe {
            let buffer_ptr = self.buffer.get();
            (*buffer_ptr).as_mut_ptr()
        };
        Some(SpmcRingBufferConsumer {
            head,
            tail,
            buffer,

            #[cfg(test)]
            id: current_consumers,
        })
    }
}

pub struct SpmcRingBufferProducer<T, const CAPACITY: usize> {
    last_slowest_head: UnsafeCell<usize>,
    heads: *mut CacheAligned<AtomicUsize>,
    tail: *const AtomicUsize,
    buffer: *mut Option<T>,

    num_consumers: *const AtomicUsize,
}

impl<T: Clone, const CAPACITY: usize> SpmcRingBufferProducer<T, CAPACITY> {
    pub fn try_push(&self, item: T) -> Result<(), T> {
        let tail = unsafe { (*self.tail).load(Ordering::Relaxed) };

        // get earliest head - cached, and if cached is full, then check updated values
        unsafe {
            let num_consumers = (&*self.num_consumers).load(Ordering::Acquire);
            let last_slowest_head_ptr = self.last_slowest_head.get();
            let last_slowest_head = last_slowest_head_ptr.read();
            if tail - last_slowest_head == CAPACITY {
                let current_slowest_head = {
                    let mut slowest_head = MAX;
                    let mut head = self.heads;
                    for _ in 0..num_consumers {
                        let head_ptr = &*head;
                        // in consumer - there is no write so no reordering is required
                        let head_idx = head_ptr.load(Ordering::Relaxed);
                        slowest_head = slowest_head.min(head_idx);
                        head = head.add(1); // not sure if need to add actual number of bytes - need to
                        // confirm
                    }
                    slowest_head
                };
                if last_slowest_head == current_slowest_head {
                    return Err(item);
                }
                last_slowest_head_ptr.write(current_slowest_head);
            }
        };

        // write data
        unsafe {
            // let buffer = buffer_ref.get() as *mut Option<T>;
            let position_ptr = self.buffer.add(tail & (CAPACITY - 1));
            position_ptr.write(Some(item));
        }

        // update tail pointer
        unsafe {
            (*self.tail).store(tail + 1_usize, Ordering::Release);
        }

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

pub struct SpmcRingBufferConsumer<T, const CAPACITY: usize> {
    head: *mut CacheAligned<AtomicUsize>,
    tail: *const AtomicUsize,
    buffer: *mut Option<T>,

    #[cfg(test)]
    id: usize,
}

impl<T: Clone, const CAPACITY: usize> SpmcRingBufferConsumer<T, CAPACITY> {
    #[cfg(test)]
    pub fn id(&self) -> usize {
        return self.id;
    }

    pub fn try_pop(&self) -> Option<T> {
        let head = unsafe { (&*self.head).load(Ordering::Relaxed) };
        let tail = unsafe { (*self.tail).load(Ordering::Acquire) };

        if tail - head == 0 {
            // ring buffer is empty
            return None;
        }

        // read data
        let val = unsafe {
            let position_ptr = self.buffer.add(head & (CAPACITY - 1));
            position_ptr.read()
        };

        // update head pointer
        unsafe {
            // syncs with previous read statement
            (&*self.head).store(head + 1_usize, Ordering::Release);
        }

        val
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
