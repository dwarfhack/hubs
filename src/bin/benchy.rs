use core::panic;
use std::{mem::replace, sync::mpsc, thread::{self, sleep}, time::{Duration, Instant}};

use hubs::{Hubs, HubsInitializer};

struct HubsInitializerU64{}
impl HubsInitializer for HubsInitializerU64{
    type T = u64;

    fn initialize_data(&self)-> Self::T {
        0
    }
}
fn main(){
    let start = Instant::now();
    test_ref(10_000_000,   1024);
    println!("Duration ref  : {}us", start.elapsed().as_micros());

    let hubs = Hubs::new(&HubsInitializerU64{});
    let start = Instant::now();
    test_stress(10_000_000,   1024, hubs);
    println!("Duration hubs : {}us", start.elapsed().as_micros());
}

fn test_ref(chunk_count: usize, data_count: usize) {

    let (mpsc_tx, mpsc_rx) =  mpsc::channel();
    let (back_tx, back_rx) =  mpsc::channel();

    let j = thread::spawn(move || {
        let mut data = Some(vec![0;4096].into_boxed_slice());
        let mut ctr = 0;
        for i in 0 .. chunk_count{
            if let Some(ref mut data) = data{
                for k in 0 .. data_count{
                    data[k] = ctr;
                    ctr += 1;
                }
            }

            let mut x = None;
            replace(&mut x, data);            
            mpsc_tx.send(x).unwrap();
            data = back_rx.recv().unwrap();
        }


        sleep(Duration::from_millis(10));

        match data{
            Some(data) => data[ctr % data_count],
            _ => panic!("aaaaah")
        }

    });

    sleep(Duration::from_millis(1));

    let mut data = Some(vec![0;4096].into_boxed_slice());


    let mut ctr = 0;
    let mut last_run = false;
    let mut tr = None;
    for i in 0 .. chunk_count{
        data = mpsc_rx.recv().unwrap();

        if let Some(ref mut data) = data{
            for k in 0 .. data_count{
                data[k] = ctr;
                ctr += 1;
            }
        }
        let mut x = None;
        replace(&mut x, data);         
        back_tx.send(x).unwrap();
    }
    if tr.is_none() {
        tr = Some(mpsc_rx.recv().unwrap());
    }

    let res = j.join().unwrap();  
    println!("res: {}, ctr: {}", res, ctr);
}


fn test_stress(chunk_count: usize, data_count: usize, hubs: Hubs<u64>) {
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
                        // sleep(Duration::from_micros(1))
                    }
                }
            }
        }
        mpsc_tx.send(0u8).unwrap();
    });

    sleep(Duration::from_millis(1));

    let mut ctr = 0;

    let mut last_run = false;
    loop  {
        for i in rx.get_chunks_for_tick().into_iter(){
            assert_eq!(*i,ctr);
            ctr +=1;
        }
        // let i = rx.get_chunks_for_tick();
        // mem::drop(i);
        if last_run {
            break
        }
        if mpsc_rx.try_recv().is_ok() {
            last_run = true;
        }                            
        // sleep(Duration::from_micros(3))
    }
   
    assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    assert_eq!(ctr as usize, chunk_count*data_count);
    j.join().unwrap();       
}