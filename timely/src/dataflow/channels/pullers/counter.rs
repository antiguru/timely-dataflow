//! A wrapper which accounts records pulled past in a shared count map.

use std::rc::Rc;
use std::cell::RefCell;

use crate::Container;
use crate::dataflow::channels::{BundleCore, MessageAllocation};
use crate::progress::ChangeBatch;
use crate::communication::Pull;

/// A wrapper which accounts records pulled past in a shared count map.
pub struct Counter<T: Ord+Clone+'static, D, A, P: Pull<BundleCore<T, D>, MessageAllocation<A>>> {
    pullable: P,
    consumed: Rc<RefCell<ChangeBatch<T>>>,
    phantom: ::std::marker::PhantomData<(D, A)>,
}

impl<T:Ord+Clone+'static, D: Container, P: Pull<BundleCore<T, D>, MessageAllocation<D::Allocation>>> Counter<T, D, D::Allocation, P> {
    /// Retrieves the next timestamp and batch of data.
    #[inline]
    pub fn next(&mut self) -> Option<(&mut BundleCore<T, D>, &mut Option<MessageAllocation<D::Allocation>>)> {
        if let (message, allocation) = self.pullable.pull() {
            if let Some(message) = message {
                if message.data.len() > 0 {
                    self.consumed.borrow_mut().update(message.time.clone(), message.data.len() as i64);
                    Some((message, allocation))
                } else { None }
            } else { None }
        } else { None }
    }
}

impl<T:Ord+Clone+'static, D, A, P: Pull<BundleCore<T, D>, MessageAllocation<A>>> Counter<T, D, A, P> {
    /// Allocates a new `Counter` from a boxed puller.
    pub fn new(pullable: P) -> Self {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable,
            consumed: Rc::new(RefCell::new(ChangeBatch::new())),
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    pub fn consumed(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.consumed
    }
}
