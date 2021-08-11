# H.U.B.S.
The Horribly Unsafe Buffer Structure

## Unsafe
This crate uses `unsafe` (as in the name).

## About
**It basically works but this code has not been used in production.**


A Data Structure that allows for fast access to pre-allocated data in chunks and allows read-access to all currently comitted chunks in one call.

This is not a general ourpose data structure, if you attempt it to use it as such, it might yield terrible performance. This crate was made for slow-ticking game loops, so that one tick every 20ms or so can easily read hundreds of thousands of items with two atomic operations. Refer to [the docs](https://docs.rs/hubs/0.1.0/hubs/)  to get started.


