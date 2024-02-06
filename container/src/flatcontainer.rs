//! Present a [`FlatStack`] as a timely container.

use flatcontainer::{CopyOnto, FlatStack, Region};
use crate::{buffer, Container, PushContainer, PushInto};

impl<R: Region + Clone + 'static> Container for FlatStack<R> {
    type ItemRef<'a> = R::ReadItem<'a>  where Self: 'a;
    type Item<'a> = R::ReadItem<'a> where Self: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear()
    }

    type Iter<'a> = <&'a Self as IntoIterator>::IntoIter;

    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        IntoIterator::into_iter(self)
    }

    type IntoIter<'a> = Self::Iter<'a>;

    fn into_iter<'a>(&'a mut self) -> Self::IntoIter<'a> {
        IntoIterator::into_iter(&*self)
    }
}

impl<R: Region + Clone + 'static> PushContainer for FlatStack<R> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn preferred_capacity() -> usize {
        buffer::default_capacity::<R::Index>()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional);
    }
}

impl<R: Region + Clone + 'static, T: CopyOnto<R>> PushInto<FlatStack<R>> for T {
    fn push_into(self, target: &mut FlatStack<R>) {
        target.copy(self);
    }
}
