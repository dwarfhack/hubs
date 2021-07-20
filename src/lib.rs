use std::{cell::UnsafeCell, fmt::Debug, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}};





const HUBS_SIZE: usize = 4096;
const CHUNK_SIZE: usize = 4096;


pub struct Hubs<T>{
    inner: HubsInner<T>
}
impl<T> Hubs<T> where T:Clone,T:Default, T:Debug{

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
                // is_write_block_borrowed: AtomicBool::new(false)
            } 
        }
    }

}

pub trait HubsInitializer{
    type T;
    fn initialize_data(&self)-> Self::T;
}

#[derive(Debug)]
pub struct Chunk<T>{
    capacity: usize,
    pub used: usize,
    pub data: Box<[T]>
}
impl<T> Chunk<T> where T:Clone,T:Default{
    fn new(initializer: &dyn HubsInitializer<T=T>) -> Self{
        let mut v = Vec::with_capacity(CHUNK_SIZE);
        for _ in 0 .. CHUNK_SIZE{
            v.push(initializer.initialize_data());
        }
        Chunk{
            capacity: 128,
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

impl<T> HubsConsumer<T> where T:Clone,T:Default, T:Debug{
    pub fn get_chunks_for_tick(&self) -> ChunkBlock<T>{
        self.inner.get_read_chunks_current()
    }
}

impl<T> HubsProducer<T> where T:Clone,T:Default, T:Debug{
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
    // is_write_block_borrowed: AtomicBool
}

pub struct ChunkBlock<'a,T>  where T:Clone,T:Default, T:Debug{
    chunks: ChunkBlockData<'a, T>,
    parent: Option<&'a HubsInner<T>>,
    current_chunk_index: usize,
    in_chunk_index: usize
}

impl <'a,T> Iterator for ChunkBlock<'a,T>  where T:Clone,T:Default, T:Debug{
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

pub struct HubsWriteAccess<'a,T> {
    pub chunk: &'a mut Chunk<T>,
    parent: &'a HubsInner<T>
}

impl <'a,T> HubsWriteAccess<'a,T> where T:Clone,T:Default, T:Debug{
    pub fn commit(self) {
        self.parent.commit_chunk(self);
    }
}



impl<T> HubsInner<T> where T:Clone, T:Default, T:Debug{
    fn get_read_chunks_current(&self) -> ChunkBlock<T>{

        // if self.is_write_block_borrowed.load(Ordering::SeqCst) {
        //     return ChunkBlock::empty()
        // }

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


        // println!("Chunks: {:?}", chunks);
        // self.is_write_block_borrowed.store(true, Ordering::SeqCst);
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

        let next_write_pos = (write_pos + 1) % HUBS_SIZE;

        self.write_ptr.store( next_write_pos, Ordering::SeqCst);
        let chunk = unsafe{ &mut(*self.chunks.get())[write_pos] };
        Some(HubsWriteAccess{
            chunk,
            parent: self,
        })
    }

    fn return_chunk_block(&self, _block: &mut ChunkBlock<T>){
        // if ! self.is_write_block_borrowed.load(Ordering::SeqCst) {
        //     panic!("Tried to return block to hubs that has no block given out")
        // }

        let read_end = self.write_barrier.load(Ordering::SeqCst);
        let mut read_ptr = self.read_ptr.load(Ordering::SeqCst);

        if read_ptr != read_end {
            panic!("Tried to return block to hubs that has no block given out")
        }

        // let mut  read_ptr = self.read_ptr.load(Ordering::SeqCst);
        read_ptr =  ( HUBS_SIZE + read_ptr - 1 ) % HUBS_SIZE;
        self.read_barrier.store(read_ptr, Ordering::SeqCst);
        // self.is_write_block_borrowed.store(false,Ordering::SeqCst)
    }

    fn commit_chunk(&self, _write_access: HubsWriteAccess<T>){
        let write_pos = self.write_ptr.load(Ordering::SeqCst);        
        let mut write_barrier = self.write_barrier.load(Ordering::SeqCst);
        write_barrier = (write_barrier + 1) % HUBS_SIZE;
        if write_pos == write_barrier {
            self.write_barrier.store(write_barrier, Ordering::SeqCst);
        }
        else  {
            panic!("Nope, cannot do that")
        }
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

    use crate::{HUBS_SIZE, HubsInitializer};

    use super::Hubs;
    
    struct HubsInitializerU64{}
    impl HubsInitializer for HubsInitializerU64{
        type T = u64;

        fn initialize_data(&self)-> Self::T {
            0
        }
    }

    #[test]
    fn test_write_single() {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        let chunk = tx.borrow_chunk_mut().unwrap();
        chunk.chunk.data[0] = 42;
        chunk.chunk.used += 1;
        chunk.commit();

        assert_eq!(* rx.get_chunks_for_tick().into_iter().next().unwrap(), 42 );
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    }

    #[test]
    fn test_dirty_write() {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        let chunk = tx.borrow_chunk_mut().unwrap();
        chunk.chunk.data[0] = 42;
        chunk.chunk.used += 1;
        
        // Dont commit, cant read anzthing

        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    }

    #[test]
    fn test_weird_api_stuff() {
        assert_eq!( Hubs::new(&HubsInitializerU64{}).split().1.get_chunks_for_tick().into_iter().into_iter().into_iter().into_iter().next(), None);        
    }

    #[test]
    fn test_basic_usage() {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        let j = thread::spawn(move || {
            for i in 0 .. 7{
                match tx.borrow_chunk_mut(){
                    Some(chunk) => {
                        for k in 0 .. 9 {
                            chunk.chunk.data[k] = i*k as u64;
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
                assert_eq!(*iter.next().unwrap(), i*k as u64);
            }
        } 
        assert_eq!(iter.next(), None);        
    }

    #[test]
    fn test_full() {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        let j = thread::spawn(move || {
            for i in 0 .. 200{
                match tx.borrow_chunk_mut(){
                    Some(chunk) => {
                        chunk.chunk.data[0] = i as u64;
                        chunk.chunk.used += 1;
                        chunk.commit();       
                    }
                    None => {
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
                assert_eq!(*iter.next().unwrap(), i as u64);
        } 
        assert_eq!(iter.next(), None);            
    }

    #[test]
    fn test_refill() {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        let (mpsc_tx, mpsc_rx) =  mpsc::channel();

        let total_msg = 1000;


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
            for i in rx.get_chunks_for_tick().into_iter(){
                assert_eq!(*i, ctr);
                // println!("---------------------------------- Item val = {} ctr = {}",*i, ctr);
                ctr +=1;
            }
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
    fn test_stress_small(){
        test_stress(      1);
        test_stress(     10);
        test_stress(    100);
        test_stress(  1_000);
        test_stress( 10_000);
        test_stress(100_000);

        test_stress(35_251);
        test_stress(23_339);


        test_stress(HUBS_SIZE  as u64 - 1);
        test_stress(HUBS_SIZE  as u64    );
        test_stress(HUBS_SIZE  as u64 + 1);

        test_stress(HUBS_SIZE  as u64 / 2);
        test_stress(HUBS_SIZE  as u64 * 2);

        test_stress_multiple_data(    1,  1);
        test_stress_multiple_data(   10, 10);
        test_stress_multiple_data  (100,100);
        test_stress_multiple_data(1_000, 10);

        test_stress_multiple_data(1_000, 128);
        test_stress_multiple_data(1_000, 127);

    }

    #[test]
    #[ignore = "takes long"]
    fn test_stress_large(){
        test_stress( 10_000_000);
    }
    
    fn test_stress(amt: u64) {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        let (mpsc_tx, mpsc_rx) =  mpsc::channel();

        let total_msg = amt;

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





    fn test_stress_multiple_data(chunk_count: usize, data_count: usize) {
        let hubs = Hubs::new(&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();
    
        let (mpsc_tx, mpsc_rx) =  mpsc::channel();
    
    
        let j = thread::spawn(move || {
            let mut ctr = 0;
            for i in 0 .. chunk_count{
                loop{
                    match tx.borrow_chunk_mut(){
                        Some(chunk) => {
                            for k in 0 .. data_count{
                                chunk.chunk.data[k] = ctr;
                                ctr += 1;
                            }
                            chunk.chunk.used = data_count;
                            chunk.commit();   
                            break
                        }
                        None => {
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
                assert_eq!(*i,ctr);
                ctr +=1;
            }
            if mpsc_rx.try_recv().is_ok() {
                break
            }                            
            sleep(Duration::from_millis(1));
        }
       
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
        assert_eq!(ctr as usize, chunk_count*data_count);
        j.join().unwrap();       
    }
}
