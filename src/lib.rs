use std::{cell::UnsafeCell, rc::Rc, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}};





const HUBS_SIZEE: usize = 128;


pub struct Hubs<T>{
    inner: HubsInner<T>
}
impl<T> Hubs<T> where T:Clone,T:Default{
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
        for i in 0..HUBS_SIZEE{
            chunks.push(Chunk::new());
        }
        let chunks = chunks.into_boxed_slice();

        Hubs{
           inner: HubsInner{
            chunks: UnsafeCell::from(chunks),
            read_barrier: AtomicUsize::new(0),
            read_ptr: AtomicUsize::new(1),
            write_ptr: AtomicUsize::new(0),
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
    is_write_block_borrowed: AtomicBool
}

struct ChunkBlock<'a,T>  where T:Clone,T:Default{
    pub chunks: &'a [Chunk<T>],
    parent: &'a HubsInner<T>
}

// impl <'a,T> ChunkBlock<'a,T> where T:Clone,T:Default{
//     pub fn return_to_hubs(&mut self){
//         self.parent.return_chunk_block(&mut self);
//     }
// }

impl <'a,T> Drop for ChunkBlock<'a,T>  where T:Clone,T:Default{
    fn drop(&mut self) {
        self.parent.return_chunk_block(self);
    }
}




impl<T> HubsInner<T> where T:Clone,T:Default{
    fn get_read_chunks_current(&self) -> Option<ChunkBlock<T>>{

        if self.is_write_block_borrowed.load(Ordering::SeqCst) {
            return None
        }

        // the easiest one: we can't read past the last written chunk
        let read_end = self.write_ptr.load(Ordering::SeqCst);

        // we need to decide where to start reading
        // this has to be the last read position
        let read_start = self.read_ptr.load(Ordering::SeqCst);
        if read_start == read_end {
            println!("Nothing to read, read_start = read_end = {}",read_start);
            return None;
        }
        else if read_start < read_end {
            println!("Readable, read_start({}) < read_end({})",read_start, read_end);

            // we can actually read something
            // we MUST set the read_end to ensure this area is not again borrowed readable (see return chunk)
            // we MUST NOT set the barrier, this has to be done upon returning data
            self.read_ptr.store(read_end, Ordering::SeqCst);
            println!("Stored read_end: {}", read_end);
        }
        else {
            panic!("Hubs got unsafe, abort")
        }

        let chunks = unsafe{ &(*self.chunks.get())[read_start..read_end+1] };
        self.is_write_block_borrowed.store(true, Ordering::SeqCst);
        Some(ChunkBlock{chunks, parent: &self})
    }

    fn borrow_chunk_mut(&self) -> Option<&mut Chunk<T>>{

        /*
            we can safely store these, since:
            - we are the only one to mutate write_ptr
            - if read_block increases while we hold the values, the wors that can happen is we return full if something was justt freed
            - read_block will not decrease what would make this unsafe
            - read_block will not increase over the read write_ptr, preventing wraparound scenarios 
         */ 
        let read_block = self.read_barrier.load(Ordering::SeqCst);
        let write_pos = self.write_ptr.load(Ordering::SeqCst);
        let next_write_pos = (write_pos + 1) % HUBS_SIZEE;
        if read_block == next_write_pos{
            println!("Full, read_block {}, write_pos {}", read_block, next_write_pos);
            return None;
        }
        else{
            println!("Not full, read_block {}, write_pos {}", read_block, next_write_pos);
        }

        self.write_ptr.store( next_write_pos, Ordering::SeqCst);
        let chunk = unsafe{ &mut(*self.chunks.get())[next_write_pos] };
        Some(chunk)
    }

    fn return_chunk_block(&self, _block: &mut ChunkBlock<T>){
        if ! self.is_write_block_borrowed.load(Ordering::SeqCst) {
            panic!("Tried to return block to hubs that has no block given out")
        }
        let mut  read_ptr = self.read_ptr.load(Ordering::SeqCst);
        read_ptr =  ( read_ptr + 1 ) % HUBS_SIZEE;
        self.read_barrier.store(read_ptr, Ordering::SeqCst);
        println!("Did update barrier to {}",read_ptr);
    }
}




#[derive(Debug)]
struct Chunk<T>{
    capacity: u32,
    used: u32,
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

impl<T> HubsConsumer<T> where T:Clone,T:Default{
    fn get_chunks_for_tick(&self) -> Option<ChunkBlock<T>>{
        self.inner.get_read_chunks_current()
    }
}

impl<T> HubsProducer<T> where T:Clone,T:Default{
    fn borrow_chunk_mut(&mut self) -> Option<&mut Chunk<T>>{
        self.inner.borrow_chunk_mut()
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

    use super::Hubs;
    

    #[test]
    fn test_compiles() {
        let hubs :Hubs<u8> = Hubs::new();
        let (mut tx,rx) = hubs.split();

        let j = thread::spawn(move || {
            for i in 0 .. 7{
                match tx.borrow_chunk_mut(){
                    Some(chunk) => {
                        chunk.data[0] = i
                    }
                    None => panic!("Must not be full")
                }
            }
        });

        let _ = j.join();

        match rx.get_chunks_for_tick(){
            Some(chunks) => {
                for i in 0 .. 7{
                    assert_eq!(chunks.chunks[i].data[0], i as u8);
                }
            }
            None => panic!("Must not be empty")
        }

        if let Some(_) = rx.get_chunks_for_tick(){
            panic!("Has to be empty")
        };
        
    }

    // #[test]
    // fn test_full() {
    //     let hubs :Hubs<u8> = Hubs::new();
    //     let (mut tx,rx) = hubs.split();

    //     let j = thread::spawn(move || {
    //         for i in 0 .. 200{
    //             match tx.borrow_chunk_mut(){
    //                 Some(chunk) => {
    //                     chunk.data[0] = i as u8
    //                 }
    //                 None => {
    //                     println!("Hubs was full at i = {}",i);
    //                     return i;
    //                 }
    //             }
    //         }
    //         return 0
    //     });

    //     let amt = j.join().unwrap();

    //     match rx.get_chunks_for_tick(){
    //         Some(chunks) => {
    //             for i in 0 .. amt{
    //                 assert_eq!(chunks.chunks[i].data[0], i as u8);
    //             }
    //         }
    //         None => panic!("Must not be empty")
    //     }

    //     if let Some(_) = rx.get_chunks_for_tick(){
    //         panic!("Has to be empty")
    //     };
        
    // }

    // #[test]
    // fn test_refill() {
    //     let hubs :Hubs<u32> = Hubs::new();
    //     let (mut tx,rx) = hubs.split();

    //     let (mpsc_tx, mpsc_rx) =  mpsc::channel();

    //     let mut total_msg = 1000;


    //     let j = thread::spawn(move || {
    //         for i in 0 .. total_msg{
    //             loop{
    //                 match tx.borrow_chunk_mut(){
    //                     Some(chunk) => {
    //                         chunk.data[0] = i;
    //                         break
    //                     }
    //                     None => {
    //                         println!("Hubs was full at i = {}",i);
    //                         sleep(Duration::from_secs(3))
    //                     }
    //                 }
    //             }

    //         }
    //         mpsc_tx.send(0u8);
    //     });

    //     sleep(Duration::from_millis(10));

    //     let mut ctr = 0;

    //     while mpsc_rx.try_recv().is_err() {
    //         match rx.get_chunks_for_tick(){
    //             Some(chunks) => {
    //                 for i in 0 .. chunks.chunks.len(){
    //                     assert_eq!(chunks.chunks[i].data[0], ctr);
    //                     ctr +=1;
    //                 }
    //             }
    //             None => println!("Tick block empty")
    //         }    
    //         sleep(Duration::from_secs(1))
    //     }
       
    //     if let Some(_) = rx.get_chunks_for_tick(){
    //         panic!("Has to be empty")
    //     };
    //     assert_eq!(ctr, total_msg);

    //     j.join().unwrap();

        
    // }
}
