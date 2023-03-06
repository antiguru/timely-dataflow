//! A `Push` implementor with a single target.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use timely_communication::Push;
use crate::{Data, Container};

use crate::dataflow::channels::BundleCore;

/// A pusher that can bind to a single downstream pusher.
pub struct PushOwned<T, D>(Rc<RefCell<Option<Box<dyn Push<BundleCore<T, D>>>>>>);

impl<T, D> PushOwned<T, D> {
    /// Create a new `PushOwned`. Similarly to `Tee`, it returns a pair where either element
    /// can be used as pusher or registrar.
    pub fn new() -> (Self, Self) {
        let zelf = Self(Rc::new(RefCell::new(None)));
        (zelf.clone(), zelf)
    }

    /// Set the downstream pusher.
    pub fn set<P: Push<BundleCore<T, D>> + 'static>(self, pusher: P) {
        *self.0.borrow_mut() = Some(Box::new(pusher));
    }
}

impl<T, D> fmt::Debug for PushOwned<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushOwned").finish_non_exhaustive()
    }
}

impl<T, D> Clone for PushOwned<T, D> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<T: Data, D: Container> Push<BundleCore<T, D>> for PushOwned<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<BundleCore<T, D>>) {
        let mut pusher = self.0.borrow_mut();
        if let Some(pusher) = pusher.as_mut() {
            pusher.push(message);
        }
    }
}
