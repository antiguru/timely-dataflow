//! A wrapper which counts the number of records pushed past and updates a shared count map.

use std::marker::PhantomData;
use std::rc::Rc;
use std::cell::RefCell;

use crate::progress::ChangeBatch;
use crate::dataflow::channels::{BundleCore, MessageAllocation};
use crate::communication::{Push, Container};

/// A wrapper which updates shared `produced` based on the number of records pushed.
#[derive(Debug)]
pub struct CounterCore<T: Ord, D: Container, P: Push<BundleCore<T, D>>> {
    pushee: P,
    produced: Rc<RefCell<ChangeBatch<T>>>,
    phantom: PhantomData<(D)>,
}

/// A counter specialized to vector.
pub type Counter<T, D, P> = CounterCore<T, Vec<D>, P>;

impl<T, D: Container, P> Push<BundleCore<T, D>> for CounterCore<T, D, P> where T : Ord+Clone+'static, P: Push<BundleCore<T, D>> {
    #[inline]
    fn push(&mut self, message: Option<BundleCore<T, D>>, allocation: &mut Option<<BundleCore<T, D> as Container>::Allocation>) {
        if let Some(message) = &message {
            self.produced.borrow_mut().update(message.time.clone(), message.data.len() as i64);
        }

        // only propagate `None` if dirty (indicates flush)
        if message.is_some() || !self.produced.borrow_mut().is_empty() {
            self.pushee.push(message, allocation);
        }
    }
}

impl<T, D: Container, P: Push<BundleCore<T, D>>> CounterCore<T, D, P> where T : Ord+Clone+'static {
    /// Allocates a new `Counter` from a pushee and shared counts.
    pub fn new(pushee: P) -> CounterCore<T, D, P> {
        CounterCore {
            pushee,
            produced: Rc::new(RefCell::new(ChangeBatch::new())),
            phantom: PhantomData,
        }
    }
    /// A references to shared changes in counts, for cloning or draining.
    #[inline]
    pub fn produced(&self) -> &Rc<RefCell<ChangeBatch<T>>> {
        &self.produced
    }
}
