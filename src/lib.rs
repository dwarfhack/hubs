//! The Horribly Unsafe Buffer Structure.
//!
//! A Data Structure that allows for fast access to pre-allocated data in chunks and allows read-access to all currently comitted chunks in one call.
//!
//! This is not a general ourpose data structure, if you attempt it to use it as such, it might yield terrible performance.
//! This crate was made for slow-ticking game loops, so that one tick every 20ms or so can easily read hundreds of thousands of items with two atomic operations.
//! Refer to [Hubs] to get started.


use std::{cell::UnsafeCell, marker::PhantomData, sync::{Arc, atomic::{ AtomicUsize, Ordering}}};

const HUBS_SIZE: usize = 256;
const CHUNK_SIZE: usize = 256;

/**
The Hubs data structure.
A `Hubs` holds a list of `Chunk`s.
Each `Chunk` contains an array of values of `T`.

# Usage
Operation works as follows:
- You create the Hubs
- You split it, to receive the [HubsProducer] and [HubsConsumer]. These can be moved to their target threads.
- You can write on the [HubsProducer] by mutably borrowing single chunks with [`.borrow_chunk_mut()`]
  - When writing an element of a chunk, it is your responsibility to set [Chunk::used]
  - You need to call [`.commit()`]: When a chunk is written, it has to be comitted. This has to be done regardnless whether it is full or not. 
  - There must be no gaps of valid data in the array within a chunk.
  Upon commit, the chunk can be read by another thread.
- To read from the Hubs, you can get a [ChunkBlock] that contains all currently committed chunks.
  - A [ChunkBlock] provides an iterator over all elements in the arrays in all chunks up to `used`.


# Example
```
    use hubs::{Hubs, HubsInitializer, HubsWriteAccess};

    let hubs = Hubs::new_default();
    let (mut tx,rx) = hubs.split();

    // In 7 Chunks, write the first 9 elements
    for i in 0 .. 7{
        match tx.borrow_chunk_mut(){
            Some(guard) => {
                for k in 0 .. 9 {
                    guard.chunk.data[k] = i*k as u64;
                    guard.chunk.used += 1;
                }
                guard.commit();
            },
            None => panic!("Must not be full")
        };
    }

    // Now read everything in one go
    let mut iter = rx.get_chunks_for_tick().into_iter();
    for i in 0 .. 7{
        for k in 0 .. 9{
            assert_eq!(*iter.next().unwrap(), i*k as u64);
        }
    } 

    assert_eq!(iter.next(), None);        

```

  [`.commit()`]: `HubsWriteAccess::commit()`
  [`.borrow_chunk_mut()`]: `HubsProducer::borrow_chunk_mut()`


*/
pub struct Hubs<T>{
    inner: HubsInner<T>
}
impl<T> Hubs<T> where T: Default{

    /// Create a [Hubs] with a [HubsInitializer] if the item type implements [Default]. 
    /// Upon initialization, all fields will contain the default value.
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



impl<T> Hubs<T>{

    /**
    Creates a new [Hubs] with the given initializer.
    The created [Hubs] has a fixed capacity to hold Chunks, that cannot be changed after creation.
    After initialization, you can use [`.split()`] to split the Hubs in a consumer and producer.
    These can be moved to another thread.

      [`.split()`]: `Hubs::split()`

    */
    pub fn new(initializer: &dyn HubsInitializer<T=T>) -> Self{
        Hubs::with_capacity(HUBS_SIZE, initializer)
    }

    /**
    The same as [`.new()`] but you can define the capacity upon creation.

    [`.new()`]: `Hubs::new()`
    */
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
        This is necessary to move one part over to another thread.
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
        Get the fixed capacity of the hubs.
        This equals the allocated chunks. Since we need a bit of space between the read and write barrier to handle the wrap around case,
        you can store one chunk less than the capacity before you need to read from the hubs.
    */
    pub fn capacity(&self) -> usize{
        self.inner.capacity
    }
}

/**
Trait used to fill the empty hubs with default data upon creation.
See [Hubs::new_default] as an alternative
Do not be afraid to implement this type, since it is used only once upon initalization.
*/
pub trait HubsInitializer{
    type T;
    fn initialize_data(&self)-> Self::T;
}


/**
    A Chunk of Data.

    A `Chunk` contains an array of your desired datatype `T`.
    The variable `used` has to be set to the amount of valid values in `data`.
    There must be no gaps in `data` but it does not has to be fully used.
    A chunk bust be comitted to be readable, see [HubsWriteAccess].

    If you did not manually clear the chunk content, there will be stale data in the chunk from the previous round.
    This is ok, if you set the `used` variable correcly.


*/
pub struct Chunk<T>{
    /// The capacity of this chunk, do not change
    pub capacity: usize,
    /// Count of used elements in this chunk. Set this before you commit
    pub used: usize,
    /// The data in this chunk. Feel free to borrow the data to other functions, e.g. a network receive
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

/**
    The producer side of the Hubs.

    Use this one for writing into the data structure.
    See [Hubs] for an overview.
    The Hubs Producer may be moved around threads.
    Do not try to do this whilst you borrowed a chunk, I haven't tested that.
*/
pub struct HubsProducer<T>{
    inner: Arc<HubsInner<T>>
}

/**
    The consumer side of the Hubs.

    Use this one for reading data from the structure.
    See [Hubs] for an overview.
    The Hubs Consumer may be moved around threads.
    Do not try to do this whilst you borrowed a set of chunks, I haven't tested that.

    To get all committed chunks, call [`.get_chunks_for_tick()`].

    You can not get only a part of these chunks.
    If you do not read all chunks retrieved in one read call, they are lost.

    [`.get_chunks_for_tick()`]: `HubsConsumer::get_chunks_for_tick`
*/
pub struct HubsConsumer<T>{
    inner: Arc<HubsInner<T>>
}


impl<T> HubsConsumer<T>{
    /**
        Gives you all currently committed Chunks in a [ChunkBlock].
        Once given out, it is your responsibility to either process them or allow them to be lost.
    */
    pub fn get_chunks_for_tick(&self) -> ChunkBlock<T>{
        self.inner.get_read_chunks_current()
    }
}

impl<T> HubsProducer<T>{
    /**
        Borrows a [Chunk] from a Hubs.
        You must give it back by calling [`.commit()`].
        There can only be one single chunk given out at any time.

        If the Hubs is full, this returns `None`.

        [`.commit()`]: `HubsWriteAccess::commit()`
    */
    pub fn borrow_chunk_mut(&mut self) -> Option<HubsWriteAccess<T>>{
        self.inner.borrow_chunk_mut()
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


/// A Block of Chunks
///
/// Acess via its Iterator
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

/**
Some sort of write guard for a single chunk.

You need to call [`.commit()`] when you are done.
This returns the chunk to the Hubs and will enable it to be read.

You may access the internal data via `chunk`. See the documentation in [Chunk] for more details

[`.commit()`]: `HubsWriteAccess::commit()`

*/
pub struct HubsWriteAccess<'a,T> {
    pub chunk: &'a mut Chunk<T>,
    parent: &'a HubsInner<T>
}

impl <'a,T> HubsWriteAccess<'a,T>{
    /**
    Commits this chunk, making it available to be read and allows the Hubs to give you a new one.
    - You **need** to call this function if you borrowed a chunk.
    - You **need** to call this function before you borrow the next chunk.
    If you fail to do so, the following will happen: Bad things.
    */
    pub fn commit(self) {
        self.parent.commit_chunk(self);
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