use std::{mem, sync::mpsc, thread::{self, sleep}, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hubs::{Hubs, HubsInitializer};


struct HubsInitializerU64{}
impl HubsInitializer for HubsInitializerU64{
    type T = u64;

    fn initialize_data(&self)-> Self::T {
        0
    }
}

fn test_reference(chunk_count: usize, data_count: usize, data: &mut [u64]) -> (u64,u64) {
    let mut ctr_a = 0;
    let mut ctr_b = 0;    
    for i in 0 .. chunk_count{    
        ctr_a = 0;
        ctr_b = 0;    
        for k in 0 .. data_count{
            data[k] = ctr_a;
            ctr_a += 1;
        }   
        for k in data.iter_mut(){
            ctr_b += *k;
            if ctr_b == 192342234234{
                panic!("badaboom")
            }
        }                   
    }
    println!("a:{}, b:{}",ctr_a,ctr_b);
    (ctr_a,ctr_b)
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
                        sleep(Duration::from_micros(1))
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
        sleep(Duration::from_micros(3))
    }
   
    assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    assert_eq!(ctr as usize, chunk_count*data_count);
    j.join().unwrap();       
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("1M * 16 churn",   |b| b.iter(|| test_reference(100_000,   16, &mut vec![0;16])));

    c.bench_function("1M * 16 churn",   |b| b.iter(|| test_stress(100_000,   16, Hubs::new(&HubsInitializerU64{}))));
    c.bench_function("1M * 128 churn",  |b| b.iter(|| test_stress(100_000,  128, Hubs::new(&HubsInitializerU64{}))));
    c.bench_function("1M * 1024 churn", |b| b.iter(|| test_stress(100_000, 1024, Hubs::new(&HubsInitializerU64{}))));
    c.bench_function("1M * 4096 churn", |b| b.iter(|| test_stress(100_000, 4096, Hubs::new(&HubsInitializerU64{}))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);