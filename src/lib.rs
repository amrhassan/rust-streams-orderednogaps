
extern crate futures;

use std::collections::BinaryHeap;
use std::cmp::Reverse;
use futures::*;

pub trait Successor {
    fn next(&self) -> Self;
}

pub struct OrderedNoGaps<S, F, K> where S: Stream {
    stream: S,
    key: F,
    last_key_polled: K,
    buffer: BinaryHeap<Reverse<S::Item>>,
}

impl<S, F, K> Stream for OrderedNoGaps<S, F, K> where
    S: Stream,
    S::Item: Ord,
    F: Fn(&S::Item) -> K,
    K: Successor + PartialEq {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if self.buffer_peek_is_next() {
            match self.buffer.pop() {
                Some(Reverse(v)) => {
                    self.last_key_polled = self.last_key_polled.next();
                    Ok(Async::Ready(Some(v)))
                },
                None => self.poll_from_stream()
            }
        } else {
            self.poll_from_stream()
        }
    }
}

impl<S, F, K> OrderedNoGaps<S, F, K> where
    S: Stream,
    S::Item: Ord,
    F: Fn(&S::Item) -> K,
    K: Successor + PartialEq {
    fn poll_from_stream(&mut self) -> Result<Async<Option<<Self as Stream>::Item>>, <Self as Stream>::Error> {
        match self.stream.poll() {
            Ok(Async::Ready(Some(v))) => {
                if self.is_next(&v) {
                    self.last_key_polled = self.last_key_polled.next();
                    Ok(Async::Ready(Some(v)))
                } else {
                    self.buffer.push(Reverse(v));
                    self.poll()
                }
            }
            otherwise => otherwise
        }
    }

    fn is_next(&self, v: &<Self as Stream>::Item) -> bool {
        let key = &self.key;
        self.last_key_polled.next() == key(v)
    }

    fn buffer_peek_is_next(&self) -> bool {
        match self.buffer.peek() {
            Some(Reverse(v)) => self.is_next(v),
            None => false
        }
    }
}

/// Given a function to retrieve the key of each value, the zero key value and an underlying stream.
///
/// Guarantees order of emitted values from the provided stream by the specified key and that no key values
/// are skipped. Halts until the next expected key is received.
///
/// *WARNING*: The system's memory is an implicit upper bound on how much this combinator can buffer internally.
pub fn ordered_no_gaps<S, F, K>(stream: S, zero: K, key: F) -> OrderedNoGaps<S, F, K> where
    S: Stream,
    S::Item: Ord,
    F: Fn(&S::Item) -> K,
    K: Successor + PartialEq {
    OrderedNoGaps { stream, key, last_key_polled: zero, buffer: BinaryHeap::default() }
}

#[cfg(test)]
mod test {

    extern crate rand;

    use super::*;
    use super::test::rand::Rng;

    impl Successor for u8 {
        fn next(&self) -> Self { self + 1 }
    }

    #[test]
    fn orders_without_gaps() {
        let mut ns: Vec<u8> = (1..100).collect();
        rand::thread_rng().shuffle(&mut ns);
        assert!(!is_ordered(ns.clone()));
        let stream: Box<Stream<Item=u8, Error=()>> = Box::new(stream::iter_ok(ns.into_iter()));
        let ordered: Vec<u8> = ordered_no_gaps(stream, 0, |&x| x).collect().wait().unwrap();
        assert!(is_ordered(ordered.clone()));
        assert!(ordered.len() == 99);
    }

    fn is_ordered(i: Vec<u8>) -> bool {
        i.into_iter().fold((true, 0), |(b, z), x| (b && x >= z, x)).0
    }
}
