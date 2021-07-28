//! The Horribly Unsafe Buffer Structure.
//!
//! A Data Structure that allows for fast access to pre-allocated data in chunks and allows read-access to all currently comitted chunks in one call.
//! This is perfect for slow-ticking game loops, since one tick every 20ms or so can easily read hundreds of thousands of items with two atomic operations.
//! Refer to [Hubs] to get started.
//! This is not a general ourpose data structure, if you attempt it to use it as such, it might yield terrible performance.


use std::{cell::UnsafeCell, marker::PhantomData, sync::{Arc, atomic::{ AtomicUsize, Ordering}}};

const HUBS_SIZE: usize = 4096;
const CHUNK_SIZE: usize = 4096;

/**
The Hubs data structure used for initialization.
Access is possible through the [HubsProducer] and [HubsConsumer].
*/
pub struct Hubs<T>{
    inner: HubsInner<T>
}
impl<T> Hubs<T> where T: Default{

    /// Create a [Hubs] with a [HubsInitializer] if the item type implements [Default]. 
    // Upon initialization, all fields will contain the default value.
    pub fn new_default() -> Self{
        let initializer = DefaultInitializer::new();
        let mut chunks = Vec::with_capacity(128);
        for _i in 0..HUBS_SIZE{
            chunks.push(Chunk::new(&initializer));
        }
        let chunks = chunks.into_boxed_slice();

        Hubs{
           inner: HubsInner{
                chunks: UnsafeCell::from(chunks),
                read_ptr: AtomicUsize::new(0),
                read_barrier: AtomicUsize::new(HUBS_SIZE-1),
                write_ptr: AtomicUsize::new(0),
                write_barrier: AtomicUsize::new(0),
                capacity: HUBS_SIZE
            } 
        }
    }
} 

struct DefaultInitializer<T>{_data: PhantomData<T>}
impl<T : Default> DefaultInitializer<T>{
    fn new() -> Self{
        DefaultInitializer{
            _data : PhantomData
        }
    }
}

impl<T : Default> HubsInitializer for DefaultInitializer<T>{
    type T=T;
    fn initialize_data(&self)-> Self::T {
        T::default()
    }
}

impl<T> Hubs<T>{

    /**
    Creates a new [Hubs] with the given initializer.
    The created [Hubs] has a fixed capacity to holt Chunks, that cannot be changed after creation.
    After initialization, you can get useable 
    */
    pub fn new(initializer: &dyn HubsInitializer<T=T>) -> Self{

        let mut chunks = Vec::with_capacity(128);
        for _i in 0..HUBS_SIZE{
            chunks.push(Chunk::new(initializer));
        }
        let chunks = chunks.into_boxed_slice();

        Hubs{
           inner: HubsInner{
                chunks: UnsafeCell::from(chunks),
                read_ptr: AtomicUsize::new(0),
                read_barrier: AtomicUsize::new(HUBS_SIZE-1),
                write_ptr: AtomicUsize::new(0),
                write_barrier: AtomicUsize::new(0),
                capacity: HUBS_SIZE
            } 
        }
    }
    pub fn with_capacity(capacity:usize, initializer: &dyn HubsInitializer<T=T>) -> Self{

        let mut chunks = Vec::with_capacity(128);
        for _i in 0..capacity{
            chunks.push(Chunk::new(initializer));
        }
        let chunks = chunks.into_boxed_slice();

        Hubs{
           inner: HubsInner{
                chunks: UnsafeCell::from(chunks),
                read_ptr: AtomicUsize::new(0),
                read_barrier: AtomicUsize::new(capacity-1),
                write_ptr: AtomicUsize::new(0),
                write_barrier: AtomicUsize::new(0),
                capacity
            } 
        }
    }

        /**
    Take Ownership of the hubs and split it.
    */
    pub fn split(self) -> (HubsProducer<T>, HubsConsumer<T>){
        let inner = Arc::new(self.inner);
        let tx = HubsProducer{
            inner: Arc::clone(&inner)
        };
        let rx = HubsConsumer{
            inner: Arc::clone(&inner)
        };
        (tx,rx)
    }

        /**
    Take Ownership of the hubs and split it.
    */
    pub fn capacity(&self) -> usize{
        self.inner.capacity
    }
}

pub trait HubsInitializer{
    type T;
    fn initialize_data(&self)-> Self::T;
}


pub struct Chunk<T>{
    pub capacity: usize,
    pub used: usize,
    pub data: Box<[T]>
}
impl<T> Chunk<T> {
    fn new(initializer: &dyn HubsInitializer<T=T>) -> Self{
        let mut v = Vec::with_capacity(CHUNK_SIZE);
        for _ in 0 .. CHUNK_SIZE{
            v.push(initializer.initialize_data());
        }
        Chunk{
            capacity: CHUNK_SIZE,
            used: 0,
            data: v.into_boxed_slice(),
        }
    }
}

pub struct HubsProducer<T>{
    inner: Arc<HubsInner<T>>
}
pub struct HubsConsumer<T>{
    inner: Arc<HubsInner<T>>
}

impl<T> HubsConsumer<T>{
    pub fn get_chunks_for_tick(&self) -> ChunkBlock<T>{
        self.inner.get_read_chunks_current()
    }
}

impl<T> HubsProducer<T>{
    pub fn borrow_chunk_mut(&mut self) -> Option<HubsWriteAccess<T>>{
        self.inner.borrow_chunk_mut()
    }
    pub fn commit_chunk(&self, write_access: HubsWriteAccess<T>){
        self.inner.commit_chunk(write_access)
    }
}

unsafe impl<T: Send> Send for HubsInner<T> {}
unsafe impl<T: Send> Sync for HubsInner<T> {}

struct HubsInner<T>{
    chunks: UnsafeCell<Box<[Chunk<T>]>>,
    read_barrier: AtomicUsize,
    read_ptr: AtomicUsize,
    write_ptr: AtomicUsize,
    write_barrier: AtomicUsize,
    capacity: usize
}

pub struct ChunkBlock<'a,T> {
    chunks: ChunkBlockData<'a, T>,
    parent: Option<&'a HubsInner<T>>,
    current_chunk_index: usize,
    in_chunk_index: usize
}

impl <'a,T> Iterator for ChunkBlock<'a,T>{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(chunk) = self.current_chunk(){
            if chunk.used > self.in_chunk_index{
                let data = &chunk.data[self.in_chunk_index];
                self.in_chunk_index += 1;
                return Some(data)
            }
            else{
                // the current chunk did not hold more data, let's retry with the next chunk
                self.current_chunk_index += 1;
                self.in_chunk_index = 0;
                if let Some(chunk) = self.current_chunk(){
                    if chunk.used > self.in_chunk_index{
                        let data = &chunk.data[self.in_chunk_index];
                        self.in_chunk_index += 1;
                        return Some(data)
                    }
                }
            }
        }
        None               
    }    
}


impl <'a,T> ChunkBlock<'a,T>{

    fn new( chunks: ChunkBlockData<'a, T>, parent: &'a HubsInner<T>) -> Self{
        ChunkBlock{
            chunks, 
            parent: Some(parent),
            current_chunk_index: 0,
            in_chunk_index: 0
        }

    }

    fn empty() -> Self{
        ChunkBlock{
            chunks: ChunkBlockData::None, 
            parent: None,
            current_chunk_index: 0,
            in_chunk_index: 0
        }

    }

    fn current_chunk(&self, ) -> Option<&'a Chunk<T>>{
        let index = self.current_chunk_index;
        match self.chunks {
            ChunkBlockData::One(block) => {
                if block.len() > index{
                    Some(&block[index])
                }
                else{
                    return None
                }
            },
            ChunkBlockData::Two(block_0, block_1) => {
                if block_0.len() > index{
                    Some(&block_0[index])
                }
                else if block_1.len() > index - block_0.len(){
                    let index = index - block_0.len();
                    Some(&block_1[index])
                }
                else{
                    return None
                }
            },
            ChunkBlockData::None => None
        }
    }

}


enum ChunkBlockData<'a,T>{
    One(&'a [Chunk<T>]),
    Two(&'a [Chunk<T>], &'a [Chunk<T>]),
    None
}


impl <'a,T> Drop for ChunkBlock<'a,T>{
    fn drop(&mut self) {
        match self.parent{
            Some(parent) =>parent.return_chunk_block(self),
            None => ()
        }
    }
}

pub struct HubsWriteAccess<'a,T> {
    pub chunk: &'a mut Chunk<T>,
    parent: &'a HubsInner<T>
}

impl <'a,T> HubsWriteAccess<'a,T>{
    pub fn commit(self) {
        self.parent.commit_chunk(self);
    }
}



impl<T> HubsInner<T>{
    fn get_read_chunks_current(&self) -> ChunkBlock<T>{

        let read_end = self.write_barrier.load(Ordering::SeqCst);
        let read_start = self.read_ptr.load(Ordering::SeqCst);

        if read_start == read_end {
            return ChunkBlock::empty()
        }
        
        self.read_ptr.store(read_end, Ordering::SeqCst);
        

        // this is tricky, we need to close the ring
        let chunks = if read_start > read_end {
            ChunkBlockData::Two(
                unsafe{ &(*self.chunks.get())[read_start..] },
                unsafe{ &(*self.chunks.get())[..read_end] }
            )
        }
        else{
            ChunkBlockData::One(unsafe{ &(*self.chunks.get())[read_start..read_end] })
        };

        ChunkBlock::new(chunks, &self)
    }

    fn borrow_chunk_mut(&self) -> Option<HubsWriteAccess<T>>{

        let write_pos = self.write_ptr.load(Ordering::SeqCst);        
        let write_barrier = self.write_barrier.load(Ordering::SeqCst);

        if write_pos!=write_barrier {
            panic!("Cant borrow more than one chunk")
        }

        let read_barrier = self.read_barrier.load(Ordering::SeqCst);

        if read_barrier == write_pos{
            return None;
        }

        let next_write_pos = (write_pos + 1) % self.capacity;

        self.write_ptr.store( next_write_pos, Ordering::SeqCst);

        /*
         SAFETY:

         */
        let chunk = unsafe{ &mut(*self.chunks.get())[write_pos] };
        Some(HubsWriteAccess{
            chunk,
            parent: self,
        })
    }

    fn return_chunk_block(&self, _block: &mut ChunkBlock<T>){

        let read_end = self.read_barrier.load(Ordering::SeqCst);
        let mut read_ptr = self.read_ptr.load(Ordering::SeqCst);

        read_ptr =  ( self.capacity + read_ptr - 1 ) % self.capacity;

        if read_ptr == read_end {
            panic!("Tried to return block to hubs that has no block given out")
        }

        self.read_barrier.store(read_ptr, Ordering::SeqCst);
    }

    fn commit_chunk(&self, _write_access: HubsWriteAccess<T>){
        let write_pos = self.write_ptr.load(Ordering::SeqCst);        
        let mut write_barrier = self.write_barrier.load(Ordering::SeqCst);
        write_barrier = (write_barrier + 1) % self.capacity;
        if write_pos == write_barrier {
            self.write_barrier.store(write_barrier, Ordering::SeqCst);
        }
        else  {
            panic!("Cant commit old chunk if already borrowed new chunk")
        }
    }
}