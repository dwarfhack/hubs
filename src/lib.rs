use std::{cell::UnsafeCell, fmt::Debug, rc::Rc, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}};





const HUBS_SIZE: usize = 128;


pub struct Hubs<T>{
    inner: HubsInner<T>
}
impl<T> Hubs<T> where T:Clone,T:Default, T:Debug{
    fn split(self) -> (HubsProducer<T>, HubsConsumer<T>){
        let inner = Arc::new(self.inner);
        let tx = HubsProducer{
            inner: Arc::clone(&inner)
        };
        let rx = HubsConsumer{
            inner: Arc::clone(&inner)
        };
        (tx,rx)
    }
    fn new() -> Self{

        let mut chunks = Vec::with_capacity(128);
        for i in 0..HUBS_SIZE{
            chunks.push(Chunk::new());
        }
        let chunks = chunks.into_boxed_slice();

        Hubs{
           inner: HubsInner{
            chunks: UnsafeCell::from(chunks),
            read_ptr: AtomicUsize::new(0),
            read_barrier: AtomicUsize::new(HUBS_SIZE-1),
            write_ptr: AtomicUsize::new(0),
            write_barrier: AtomicUsize::new(0),
            is_write_block_borrowed: AtomicBool::new(false)
        } 
        }
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
    is_write_block_borrowed: AtomicBool
}

struct ChunkBlock<'a,T>  where T:Clone,T:Default, T:Debug{
    chunks: ChunkBlockData<'a, T>,
    parent: Option<&'a HubsInner<T>>,
    current_chunk_index: usize,
    in_chunk_index: usize
}

impl <'a,T> Iterator for ChunkBlock<'a,T>  where T:Clone,T:Default, T:Debug{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        println!("Called next, chunk_idx: {}, data_idx: {}", self.current_chunk_index, self.in_chunk_index);
        if let Some(chunk) = self.current_chunk(){
            println!("Chunk found");
            if chunk.used > self.in_chunk_index{
                println!("Chunk contains next data at {}", self.in_chunk_index);
                let data = &chunk.data[self.in_chunk_index];
                self.in_chunk_index += 1;
                return Some(data)
            }
            else{
                // the current chunk did not hold more data, let's retry with the next chunk
                self.current_chunk_index += 1;
                self.in_chunk_index = 0;
                println!("Next chunk may contain data");
                if let Some(chunk) = self.current_chunk(){
                    if chunk.used > self.in_chunk_index{
                        println!("New Chunk contains next data at {}", self.in_chunk_index);
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


impl <'a,T> ChunkBlock<'a,T>  where T:Clone,T:Default, T:Debug{

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
                println!("Working with ONE chunk, len: {}, idx: {}", block.len(),index);
                if block.len() > index{
                    Some(&block[index])
                }
                else{
                    return None
                }
            },
            ChunkBlockData::Two(block_0, block_1) => {
                println!("Working with TWO chunks, len_0: {}, len_1: {}, idx: {}", block_0.len(), block_1.len(), index);
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


enum ChunkBlockData<'a,T>  where T:Clone,T:Default, T:Debug{
    One(&'a [Chunk<T>]),
    Two(&'a [Chunk<T>], &'a [Chunk<T>]),
    None
}



// impl <'a,T> ChunkBlock<'a,T> where T:Clone,T:Default{
//     pub fn return_to_hubs(&mut self){
//         self.parent.return_chunk_block(&mut self);
//     }
// }

impl <'a,T> Drop for ChunkBlock<'a,T>  where T:Clone,T:Default, T:Debug{
    fn drop(&mut self) {
        match self.parent{
            Some(parent) =>parent.return_chunk_block(self),
            None => ()
        }
    }
}

struct HubsWriteAccess<'a,T> {
    pub chunk: &'a mut Chunk<T>,
    parent: &'a HubsInner<T>
}

impl <'a,T> HubsWriteAccess<'a,T> where T:Clone,T:Default, T:Debug{
    fn commit(self) {
        self.parent.commit_chunk(self);
    }
}



impl<T> HubsInner<T> where T:Clone, T:Default, T:Debug{
    fn get_read_chunks_current(&self) -> ChunkBlock<T>{

        if self.is_write_block_borrowed.load(Ordering::SeqCst) {
            println!("----- Already borrowed");
            return ChunkBlock::empty()
        }

        let read_end = self.write_barrier.load(Ordering::SeqCst);
        let read_start = self.read_ptr.load(Ordering::SeqCst);
        println!("----- Trying to read,  read_start({}) < read_end({})",read_start, read_end);

        if read_start == read_end {
            println!("Nothing to read, read_start = read_end = {}",read_start);
            return ChunkBlock::empty()
        }
        // else if read_start < read_end {
            println!("Readable, read_start({}) < read_end({})",read_start, read_end);

            // we can actually read something
            // we MUST set the read_end to ensure this area is not again borrowed readable (see return chunk)
            // we MUST NOT set the barrier, this has to be done upon returning data
            self.read_ptr.store(read_end, Ordering::SeqCst);
            // println!("Stored read_end: {}", read_end);
        // }
        // else {
        //     panic!("Hubs got unsafe, abort")
        // }

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


        // println!("Chunks: {:?}", chunks);
        self.is_write_block_borrowed.store(true, Ordering::SeqCst);
        ChunkBlock::new(chunks, &self)
    }

    fn borrow_chunk_mut(&self) -> Option<HubsWriteAccess<T>>{

        let write_pos = self.write_ptr.load(Ordering::SeqCst);        
        let write_barrier = self.write_barrier.load(Ordering::SeqCst);

        // println!("Full, write_pos {}, write_barrier {}", write_pos, write_barrier);

        if write_pos!=write_barrier {
            panic!("Cant borrow more than one chunk")
        }

        let read_barrier = self.read_barrier.load(Ordering::SeqCst);

        if read_barrier == write_pos{
            println!("Full, read_barrier {}, write_pos {}, write_barrier{}", read_barrier, write_pos, write_barrier);
            return None;
        }
        else{
            println!("Not full, read_barrier {}, write_pos {}, write_barrier{}", read_barrier, write_pos, write_barrier);
        }

        let next_write_pos = (write_pos + 1) % HUBS_SIZE;

        self.write_ptr.store( next_write_pos, Ordering::SeqCst);
        let chunk = unsafe{ &mut(*self.chunks.get())[write_pos] };
        Some(HubsWriteAccess{
            chunk,
            parent: self,
        })
    }

    fn return_chunk_block(&self, _block: &mut ChunkBlock<T>){
        if ! self.is_write_block_borrowed.load(Ordering::SeqCst) {
            panic!("Tried to return block to hubs that has no block given out")
        }
        let mut  read_ptr = self.read_ptr.load(Ordering::SeqCst);
        read_ptr =  ( HUBS_SIZE + read_ptr - 1 ) % HUBS_SIZE;
        self.read_barrier.store(read_ptr, Ordering::SeqCst);
        println!("Did update read barrier to {}",read_ptr);
        self.is_write_block_borrowed.store(false,Ordering::SeqCst)
    }

    fn commit_chunk(&self, write_access: HubsWriteAccess<T>){
        let write_pos = self.write_ptr.load(Ordering::SeqCst);        
        let mut write_barrier = self.write_barrier.load(Ordering::SeqCst);
        write_barrier = (write_barrier + 1) % HUBS_SIZE;
        if write_pos == write_barrier {
            self.write_barrier.store(write_barrier, Ordering::SeqCst);
            // println!("Commit, write_barrier {}, chunk: {:?}", write_barrier ,write_access.chunk);

        }
        else  {
            panic!("Nope, cannot do that")
        }
    }
}




#[derive(Debug)]
struct Chunk<T>{
    capacity: usize,
    used: usize,
    data: Box<[T]>
}
impl<T> Chunk<T> where T:Clone,T:Default{
    fn new() -> Self{
        Chunk{
            capacity: 128,
            used: 0,
            data: vec![T::default();128].into_boxed_slice(),
        }
    }
}

struct HubsProducer<T>{
    inner: Arc<HubsInner<T>>
}
struct HubsConsumer<T>{
    inner: Arc<HubsInner<T>>
}

impl<T> HubsConsumer<T> where T:Clone,T:Default, T:Debug{
    fn get_chunks_for_tick(&self) -> ChunkBlock<T>{
        self.inner.get_read_chunks_current()
    }
}

impl<T> HubsProducer<T> where T:Clone,T:Default, T:Debug{
    fn borrow_chunk_mut(&mut self) -> Option<HubsWriteAccess<T>>{
        self.inner.borrow_chunk_mut()
    }
    fn commit_chunk(&self, write_access: HubsWriteAccess<T>){
        self.inner.commit_chunk(write_access)
    }
}


// unsafe cell
// arc
// tick insert/retrieve
// non-overlapping elements based on tick?
// preallocated slab like buffers
// chunks of checkout/plcae back buffers



#[cfg(test)]
mod tests {

    use std::{sync::mpsc, thread::{self, sleep}, time::Duration};

    use crate::HUBS_SIZE;

    use super::Hubs;
    
    #[test]
    fn test_write_single() {
        let hubs :Hubs<u8> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let chunk = tx.borrow_chunk_mut().unwrap();
        chunk.chunk.data[0] = 42;
        chunk.chunk.used += 1;
        chunk.commit();

        assert_eq!(* rx.get_chunks_for_tick().into_iter().next().unwrap(), 42 as u8);
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    }

    #[test]
    fn test_dirty_write() {
        let hubs :Hubs<u8> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let chunk = tx.borrow_chunk_mut().unwrap();
        chunk.chunk.data[0] = 42;
        chunk.chunk.used += 1;
        
        // Dont commit, cant read anzthing

        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    }

    #[test]
    fn test_weird_api_stuff() {
        assert_eq!( Hubs::<()>::new().split().1.get_chunks_for_tick().into_iter().into_iter().into_iter().into_iter().next(), None);        
    }

    #[test]
    fn test_basic_usage() {
        let hubs :Hubs<u128> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let j = thread::spawn(move || {
            for i in 0 .. 7{
                match tx.borrow_chunk_mut(){
                    Some(chunk) => {
                        for k in 0 .. 9 {
                            chunk.chunk.data[k] = i*k as u128;
                            chunk.chunk.used += 1;
                        }
                        chunk.commit();
                    }
                    None => panic!("Must not be full")
                };
            }
        });

        let _ = j.join();

        let mut iter = rx.get_chunks_for_tick().into_iter();
        for i in 0 .. 7{
            for k in 0 .. 9{
                assert_eq!(*iter.next().unwrap(), i*k as u128);
            }
        } 
        assert_eq!(iter.next(), None);        
    }

    #[test]
    fn test_full() {
        let hubs :Hubs<u8> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let j = thread::spawn(move || {
            for i in 0 .. 200{
                match tx.borrow_chunk_mut(){
                    Some(chunk) => {
                        chunk.chunk.data[0] = i as u8;
                        chunk.chunk.used += 1;
                        chunk.commit();       
                    }
                    None => {
                        println!("Hubs was full at i = {}",i);
                        return i;
                    }
                }
            }
            return 0
        });

        let amt = j.join().unwrap();
        assert_eq!(amt,HUBS_SIZE-1);

        let mut iter = rx.get_chunks_for_tick().into_iter();
        for i in 0 .. amt{
                assert_eq!(*iter.next().unwrap(), i as u8);
        } 
        assert_eq!(iter.next(), None);            
    }

    #[test]
    fn test_refill() {
        let hubs :Hubs<u32> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let (mpsc_tx, mpsc_rx) =  mpsc::channel();

        let total_msg = 1000;


        let j = thread::spawn(move || {
            for i in 0 .. total_msg{
                loop{
                    match tx.borrow_chunk_mut(){
                        Some(chunk) => {
                            println!("writingg with {}", i);
                            chunk.chunk.data[0] = i ;
                            chunk.chunk.used = 1;
                            chunk.commit();   
                            break
                        }
                        None => {
                            println!("Hubs was full at i = {}",i);
                            sleep(Duration::from_millis(10))
                        }
                    }
                }

            }
            mpsc_tx.send(0u8);
        });

        sleep(Duration::from_millis(1));

        let mut ctr = 0;

        loop  {
            println!("looping");
            for i in rx.get_chunks_for_tick().into_iter(){
                assert_eq!(*i, ctr);
                // println!("---------------------------------- Item val = {} ctr = {}",*i, ctr);
                ctr +=1;
            }
            println!("Tick block finished, ctr = {}", ctr);
            if mpsc_rx.try_recv().is_ok() {
                // empty and other thread signals that they are done -> we are done too
                break
            }
            sleep(Duration::from_millis(3))
        }
       
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        

        assert_eq!(ctr, total_msg);

        j.join().unwrap();        
    }

    #[test]
    fn test_stress() {
        let hubs :Hubs<u32> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let (mpsc_tx, mpsc_rx) =  mpsc::channel();

        let total_msg = 1_000_000;

        let j = thread::spawn(move || {
            for i in 0 .. total_msg{
                loop{
                    match tx.borrow_chunk_mut(){
                        Some(chunk) => {
                            // println!("writingg with {}", i);
                            chunk.chunk.data[0] = i ;
                            chunk.chunk.used = 1;
                            chunk.commit();   
                            break
                        }
                        None => {
                            // println!("Hubs was full at i = {}",i);
                            sleep(Duration::from_nanos(10))
                        }
                    }
                }
            }
            mpsc_tx.send(0u8).unwrap();
        });

        sleep(Duration::from_millis(1));

        let mut ctr = 0;

        loop  {
            for i in rx.get_chunks_for_tick().into_iter(){
                assert_eq!(*i, ctr);
                ctr +=1;
            }
            if mpsc_rx.try_recv().is_ok() {
                break
            }                            
            sleep(Duration::from_nanos(7))
        }
       
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
        assert_eq!(ctr, total_msg);
        j.join().unwrap();        

    }
}
