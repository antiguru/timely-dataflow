//! Present a [`FlatStack`] as a timely container.

use flatcontainer::{FlatStack, Region};
use crate::Container;

impl<R: Region + Clone + 'static> Container for FlatStack<R> {
    fn len(&self) -> usize {
        todo!()
    }

    fn clear(&mut self) {
        todo!()
    }
}
