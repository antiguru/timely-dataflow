//! A simple communication infrastructure providing typed exchange channels.
//!
//! This crate is part of the timely dataflow system, used primarily for its inter-worker communication.
//! It may be independently useful, but it is separated out mostly to make clear boundaries in the project.
//!
//! Threads are spawned with an [`allocator::Generic`](allocator::generic::Generic), whose
//! [`allocate`](allocator::generic::Generic::allocate) method returns a pair of several send endpoints and one
//! receive endpoint. Messages sent into a send endpoint will eventually be received by the corresponding worker,
//! if it receives often enough. The point-to-point channels are each FIFO, but with no fairness guarantees.
//!
//! To be communicated, a type must implement the [`Serialize`](serde::Serialize) trait when using the
//! `bincode` feature or the [`Abomonation`](abomonation::Abomonation) trait when not.
//!
//! Channel endpoints also implement a lower-level `push` and `pull` interface (through the [`Push`](Push) and [`Pull`](Pull)
//! traits), which is used for more precise control of resources.
//!
//! # Examples
//! ```
//! use timely_communication::Allocate;
//!
//! // configure for two threads, just one process.
//! let config = timely_communication::Config::Process(2);
//!
//! // initializes communication, spawns workers
//! let guards = timely_communication::initialize(config, |mut allocator| {
//!     println!("worker {} started", allocator.index());
//!
//!     // allocates a pair of senders list and one receiver.
//!     let (mut senders, mut receiver) = allocator.allocate(0);
//!
//!     // send typed data along each channel
//!     use timely_communication::Message;
//!     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
//!     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
//!
//!     // no support for termination notification,
//!     // we have to count down ourselves.
//!     let mut expecting = 2;
//!     while expecting > 0 {
//!
//!         allocator.receive();
//!         if let Some(message) = receiver.recv() {
//!             use std::ops::Deref;
//!             println!("worker {}: received: <{}>", allocator.index(), message.deref());
//!             expecting -= 1;
//!         }
//!         allocator.release();
//!     }
//!
//!     // optionally, return something
//!     allocator.index()
//! });
//!
//! // computation runs until guards are joined or dropped.
//! if let Ok(guards) = guards {
//!     for guard in guards.join() {
//!         println!("result: {:?}", guard);
//!     }
//! }
//! else { println!("error in computation"); }
//! ```
//!
//! The should produce output like:
//!
//! ```ignore
//! worker 0 started
//! worker 1 started
//! worker 0: received: <hello, 0>
//! worker 1: received: <hello, 1>
//! worker 0: received: <hello, 0>
//! worker 1: received: <hello, 1>
//! result: Ok(0)
//! result: Ok(1)
//! ```

#![forbid(missing_docs)]

#[cfg(feature = "getopts")]
extern crate getopts;
#[cfg(feature = "bincode")]
extern crate bincode;
#[cfg(feature = "bincode")]
extern crate serde;

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

extern crate timely_bytes as bytes;
extern crate timely_logging as logging_core;

pub mod allocator;
pub mod networking;
pub mod initialize;
pub mod logging;
pub mod message;
pub mod buzzer;

use std::any::Any;

#[cfg(feature = "bincode")]
use serde::{Serialize, Deserialize};
#[cfg(not(feature = "bincode"))]
use abomonation::Abomonation;

pub use allocator::Generic as Allocator;
pub use allocator::Allocate;
pub use initialize::{initialize, initialize_from, Config, WorkerGuards};
pub use message::Message;

/// A composite trait for types that may be used with channels.
#[cfg(not(feature = "bincode"))]
pub trait Data : Send+Sync+Any+Abomonation+'static { }
#[cfg(not(feature = "bincode"))]
impl<T: Send+Sync+Any+Abomonation+'static> Data for T { }

/// A composite trait for types that may be used with channels.
#[cfg(feature = "bincode")]
pub trait Data : Send+Sync+Any+Serialize+for<'a>Deserialize<'a>+'static { }
#[cfg(feature = "bincode")]
impl<T: Send+Sync+Any+Serialize+for<'a>Deserialize<'a>+'static> Data for T { }

/// Pushing elements of type `T`.
///
/// This trait moves data around using references rather than ownership,
/// which provides the opportunity for zero-copy operation. In the call
/// to `push(element)` the implementor can *swap* some other value to
/// replace `element`, effectively returning the value to the caller.
///
/// Conventionally, a sequence of calls to `push()` should conclude with
/// a call of `push(&mut None)` or `done()` to signal to implementors that
/// another call to `push()` may not be coming.
pub trait Push<T: Container> {
    /// Pushes `element` with the opportunity to take ownership.
    fn push(&mut self, element: Option<T>, allocation: &mut Option<T::Allocation>);
    /// Pushes `element` and drops any resulting resources.
    #[inline]
    fn send(&mut self, element: T) { self.push(Some(element), &mut None); }
    /// Pushes `None`, conventionally signalling a flush.
    #[inline]
    fn done(&mut self) { self.push(None, &mut None); }
}

impl<T: Container, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, element: Option<T>, allocation: &mut Option<T::Allocation>) { (**self).push(element, allocation) }
}

/// Pulling elements of type `T`.
pub trait Pull<T: Container> {
    /// Pulls an element and provides the opportunity to take ownership.
    ///
    /// The puller may mutate the result, in particular take ownership of the data by
    /// replacing it with other data or even `None`. This allows the puller to return
    /// resources to the implementor.
    ///
    /// If `pull` returns `None` this conventionally signals that no more data is available
    /// at the moment, and the puller should find something better to do.
    fn pull(&mut self) -> (Option<T>, &mut Option<T::Allocation>);
    /// Takes an `Option<T>` and leaves `None` behind.
    #[inline]
    fn recv(&mut self) -> Option<T> { self.pull().0.take() }
}

impl<T: Container, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline]
    fn pull(&mut self) -> (Option<T>, &mut Option<T::Allocation>) { (**self).pull() }
}

/// TODO
pub trait Container: Sized {

    /// TODO
    type Allocation: IntoAllocated<Self> + 'static;
    // type Allocation: 'static;

    /// TODO
    fn hollow(self) -> Self::Allocation;

    /// TODO
    fn len(&self) -> usize;

    ///
    fn is_empty(&self) -> bool;
}

impl Container for () {
    type Allocation = ();
    fn hollow(self) -> Self::Allocation {
        ()
    }

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<A: Container> Container for (A,) {
    type Allocation = (A::Allocation,);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), )
    }

    fn len(&self) -> usize {
       self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<A: Container, B: Container> Container for (A, B) {
    type Allocation = (A::Allocation, B::Allocation);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), self.1.hollow())
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty()
    }
}

impl<A: Container, B: Container, C: Container> Container for (A, B, C) {
    type Allocation = (A::Allocation, B::Allocation, C::Allocation);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), self.1.hollow(), self.2.hollow())
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len() + self.2.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty() && self.2.is_empty()
    }
}

impl<A: Container, B: Container, C: Container, D: Container> Container for (A, B, C, D) {
    type Allocation = (A::Allocation, B::Allocation, C::Allocation, D::Allocation);
    fn hollow(self) -> Self::Allocation {
        (self.0.hollow(), self.1.hollow(), self.2.hollow(), self.3.hollow())
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len() + self.2.len() + self.3.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty() && self.2.is_empty() && self.3.is_empty()
    }
}

impl Container for usize {
    type Allocation = ();
    fn hollow(self) -> Self::Allocation {
        ()
    }

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl Container for String {
    type Allocation = String;

    fn hollow(mut self) -> Self::Allocation {
        self.clear();
        self
    }

    fn len(&self) -> usize {
        String::len(self)
    }

    fn is_empty(&self) -> bool {
        String::is_empty(self)
    }
}

use crossbeam_channel::{Sender, Receiver};
use crate::message::IntoAllocated;

/// Allocate a matrix of send and receive changes to exchange items.
///
/// This method constructs channels for `sends` threads to create and send
/// items of type `T` to `recvs` receiver threads.
fn promise_futures<T>(sends: usize, recvs: usize) -> (Vec<Vec<Sender<T>>>, Vec<Vec<Receiver<T>>>) {

    // each pair of workers has a sender and a receiver.
    let mut senders: Vec<_> = (0 .. sends).map(|_| Vec::with_capacity(recvs)).collect();
    let mut recvers: Vec<_> = (0 .. recvs).map(|_| Vec::with_capacity(sends)).collect();

    for sender in 0 .. sends {
        for recver in 0 .. recvs {
            let (send, recv) = crossbeam_channel::unbounded();
            senders[sender].push(send);
            recvers[recver].push(recv);
        }
    }

    (senders, recvers)
}

impl<T: Clone + 'static> Container for Vec<T> {
    type Allocation = Self;

    fn hollow(mut self) -> Self::Allocation {
        self.clear();
        self
    }

    fn len(&self) -> usize {
        Vec::len(&self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(&self)
    }
}