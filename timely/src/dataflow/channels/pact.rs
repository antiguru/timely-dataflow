//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: AsWorker`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::{fmt::{self, Debug}, marker::PhantomData};

use crate::communication::{Push, Pull, Data};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::communication::Message as CommMessage;

use crate::worker::AsWorker;
use crate::{Container, DrainContainer, ExchangeContainer, ExchangeData};
use crate::dataflow::channels::pushers::Exchange as ExchangePusher;
use super::{BundleCore, Message};

use crate::logging::{TimelyLogger as Logger, MessagesEvent};

/// A `ParallelizationContractCore` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContractCore<T: 'static, D: Container> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<BundleCore<T, D>, CommMessage<D::Allocation>>+'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<BundleCore<T, D>, CommMessage<D::Allocation>>+'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller);
}

/// A `ParallelizationContractCore` specialized for `Vec` containers
/// TODO: Use trait aliases once stable.
pub trait ParallelizationContract<T: 'static, D: Clone + 'static>: ParallelizationContractCore<T, Vec<D>> { }

/// A direct connection
#[derive(Debug)]
pub struct Pipeline;

impl<T: 'static, D: Container+'static> ParallelizationContractCore<T, D> for Pipeline {
    type Pusher = LogPusher<T, D, D::Allocation, ThreadPusher<BundleCore<T, D>, CommMessage<D::Allocation>>>;
    type Puller = LogPuller<T, D, D::Allocation, ThreadPuller<BundleCore<T, D>, CommMessage<D::Allocation>>>;
    fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (pusher, puller) = allocator.pipeline::<Message<T, D>, D::Allocation>(identifier, address);
        // // ignore `&mut A` and use thread allocator
        // let (pusher, puller) = Thread::new::<Bundle<T, D>>();
        (LogPusher::new(pusher, allocator.index(), allocator.index(), identifier, logging.clone()),
         LogPuller::new(puller, allocator.index(), identifier, logging))
    }
}

impl<T: 'static, D: Clone+'static> ParallelizationContract<T, D> for Pipeline { }

/// An exchange between multiple observers by data
pub struct Exchange<C, D, F> { hash_func: F, phantom: PhantomData<(C, D)> }

impl<'a, C: Container, D: Data, F: FnMut(&D)->u64+'static> Exchange<C, D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> Exchange<C, D, F> {
        Exchange {
            hash_func:  func,
            phantom:    PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<'a, T: Eq+Data+Clone, C: Container<Inner=D>+ExchangeContainer+'static, D: Data+Clone, F: FnMut(&D)->u64+'static> ParallelizationContractCore<T, C> for Exchange<C, D, F>
    where for<'b> &'b mut C: DrainContainer<Inner=D>,
        C::Allocation: ExchangeData,
{
    // TODO: The closure in the type prevents us from naming it.
    //       Could specialize `ExchangePusher` to a time-free version.
    type Pusher = Box<dyn Push<BundleCore<T, C>, CommMessage<C::Allocation>>>;
    type Puller = Box<dyn Pull<BundleCore<T, C>, CommMessage<C::Allocation>>>;
    fn connect<A: AsWorker>(mut self, allocator: &mut A, identifier: usize, address: &[usize], logging: Option<Logger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, C>, C::Allocation>(identifier, address);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        (Box::new(ExchangePusher::new(senders, move |_, d| (self.hash_func)(d))), Box::new(LogPuller::new(receiver, allocator.index(), identifier, logging.clone())))
    }
}

impl<T: Eq+Data+Clone, D: Data+Clone, F: FnMut(&D)->u64+'static> ParallelizationContract<T, D> for Exchange<Vec<D>, D, F> { }

impl<C, D, F> Debug for Exchange<C, D, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchange").finish()
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPusher<T, D, A, P: Push<BundleCore<T, D>, CommMessage<A>>> {
    pusher: P,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
    phantom: PhantomData<(T, D, A)>,
    logging: Option<Logger>,
}

impl<T, D, A, P: Push<BundleCore<T, D>, CommMessage<A>>> LogPusher<T, D, A, P> {
    /// Allocates a new pusher.
    pub fn new(pusher: P, source: usize, target: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPusher {
            pusher,
            channel,
            counter: 0,
            source,
            target,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, D: Container, P: Push<BundleCore<T, D>, CommMessage<D::Allocation>>> Push<BundleCore<T, D>, CommMessage<D::Allocation>> for LogPusher<T, D, D::Allocation, P> {
    #[inline]
    fn push(&mut self, pair: Option<BundleCore<T, D>>, allocation: &mut Option<CommMessage<D::Allocation>>) {
        if let Some(bundle) = pair {
            self.counter += 1;

            // Stamp the sequence number and source.
            // FIXME: Awkward moment/logic.
            if let Some(message) = bundle.if_mut() {
                message.seq = self.counter - 1;
                message.from = self.source;
            }

            if let Some(logger) = self.logging.as_ref() {
                logger.log(MessagesEvent {
                    is_send: true,
                    channel: self.channel,
                    source: self.source,
                    target: self.target,
                    seq_no: self.counter - 1,
                    length: bundle.data.len(),
                })
            }
        }

        self.pusher.push(pair);
    }
}

/// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPuller<T, D, A, P: Pull<BundleCore<T, D>, CommMessage<A>>> {
    puller: P,
    channel: usize,
    index: usize,
    phantom: PhantomData<(T, D, A)>,
    logging: Option<Logger>,
}

impl<T, D, A, P: Pull<BundleCore<T, D>, CommMessage<A>>> LogPuller<T, D, A, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPuller {
            puller,
            channel,
            index,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, D: Container, P: Pull<BundleCore<T, D>, CommMessage<D::Allocation>>> Pull<BundleCore<T, D>, CommMessage<D::Allocation>> for LogPuller<T, D, D::Allocation, P> {
    #[inline]
    fn pull(&mut self) -> &mut (Option<BundleCore<T,D>>, Option<CommMessage<D::Allocation>>) {
        let result = self.puller.pull();
        if let (Some(bundle), allocation) = result {
            let channel = self.channel;
            let target = self.index;

            if let Some(logger) = self.logging.as_ref() {
                logger.log(MessagesEvent {
                    is_send: false,
                    channel,
                    source: bundle.from,
                    target,
                    seq_no: bundle.seq,
                    length: bundle.data.len(),
                });
            }
        }

        result
    }
}
