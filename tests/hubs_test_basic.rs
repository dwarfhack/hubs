

#[cfg(test)]
mod tests {

    use std::{sync::mpsc, thread::{self, sleep}, time::Duration};

    use hubs::HubsInitializer;

    use hubs::Hubs;
    
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
        let cap = hubs.capacity();
        let (mut tx,rx) = hubs.split();

        let j = thread::spawn(move || {
            for i in 0 .. cap{
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
            return 0 // overfull
        });

        let amt = j.join().unwrap();
        assert_eq!(amt,cap-1);

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

        let default_hubs_cap = Hubs::new(&HubsInitializerU64{}).capacity();

        test_stress(      1);
        test_stress(     10);
        test_stress(    100);
        test_stress(  1_000);
        test_stress( 10_000);
        test_stress(100_000);

        test_stress(35_251);
        test_stress(23_339);


        test_stress(default_hubs_cap  as u64 - 1);
        test_stress(default_hubs_cap  as u64    );
        test_stress(default_hubs_cap  as u64 + 1);

        test_stress(default_hubs_cap  as u64 / 2);
        test_stress(default_hubs_cap  as u64 * 2);

        test_stress_multiple_data(    1,  1);
        test_stress_multiple_data(   10, 10);
        test_stress_multiple_data(  100,100);
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

        let mut last_try=false;
        while !last_try  {
            if mpsc_rx.try_recv().is_ok() {
                last_try = true;
                sleep(Duration::from_millis(7))
            }     
            for i in rx.get_chunks_for_tick().into_iter(){
                assert_eq!(*i, ctr);
                ctr +=1;
            }       
            sleep(Duration::from_nanos(10))      
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
                            for k in 0 .. usize::min(chunk.chunk.capacity ,data_count){
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
    
        let mut last_try=false;
        while !last_try  {
            if mpsc_rx.try_recv().is_ok() {
                last_try = true;
                sleep(Duration::from_millis(7))
            }     
            for i in rx.get_chunks_for_tick().into_iter(){
                assert_eq!(*i, ctr);
                ctr +=1;
            }          
            sleep(Duration::from_nanos(10))   
        }
       
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
        assert_eq!(ctr as usize, chunk_count*data_count);
        j.join().unwrap();       
    }

    #[test]
    fn test_with_capacity() {
        let hubs = Hubs::with_capacity(3,&HubsInitializerU64{});
        let (mut tx,rx) = hubs.split();

        for i in 0 ..2 {
            let chunk = tx.borrow_chunk_mut().unwrap();
            chunk.chunk.data[0] = i;
            chunk.chunk.used += 1;
            chunk.commit();
        }

        assert!(tx.borrow_chunk_mut().is_none(),"Hubs has to be full with custom capacity");

        let mut iter = rx.get_chunks_for_tick().into_iter();
        assert_eq!(* iter.next().unwrap(), 0 );
        assert_eq!(* iter.next().unwrap(), 1 );
        assert_eq!(iter.next(), None );
        assert_eq!( rx.get_chunks_for_tick().into_iter().next(), None);        
    }

    #[test]
    fn test_with_default() {
        let hubs = Hubs::<u64>::new_default();
        let (mut tx,_) = hubs.split();

        let chunk = tx.borrow_chunk_mut().unwrap();
        let old_value = chunk.chunk.data[0];
        assert_eq!(old_value, 0)
     
    }
}

