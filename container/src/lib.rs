//! Specifications for containers

#![forbid(missing_docs)]

pub mod columnation;
mod flatcontainer;

/// A container transferring data through dataflow edges
///
/// A container stores a number of elements and thus is able to describe it length (`len()`) and
/// whether it is empty (`is_empty()`). It supports removing all elements (`clear`).
///
/// A container must implement default. The default implementation is not required to allocate
/// memory for variable-length components.
///
/// We require the container to be cloneable to enable efficient copies when providing references
/// of containers to operators. Care must be taken that the type's `clone_from` implementation
/// is efficient (which is not necessarily the case when deriving `Clone`.)
/// TODO: Don't require `Container: Clone`
pub trait Container: Default + Clone + 'static {
    /// The type of elements this container holds.
    type ItemRef<'a> where Self: 'a;

    /// The type of elements this container holds.
    type Item<'a> where Self: 'a;

    /// The number of elements in this container
    ///
    /// The length of a container must be consistent between sending and receiving it.
    /// When exchanging a container and partitioning it into pieces, the sum of the length
    /// of all pieces must be equal to the length of the original container.
    fn len(&self) -> usize;

    /// Determine if the container contains any elements, corresponding to `len() == 0`.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all contents from `self` while retaining allocated memory.
    /// After calling `clear`, `is_empty` must return `true` and `len` 0.
    fn clear(&mut self);

    /// TODO
    type Iter<'a>: IntoIterator<Item=Self::ItemRef<'a>>;
    /// TODO
    fn iter<'a>(&'a self) -> Self::Iter<'a>;

    /// TODO
    type IntoIter<'a>: IntoIterator<Item=Self::Item<'a>>;
    /// TODO
    fn into_iter<'a>(&'a mut self) -> Self::IntoIter<'a>;
}

/// TODO
pub trait PushInto<C> {
    /// TODO
    fn push_into(self, target: &mut C);
}

/// TODO
pub trait PushContainer {
    /// TODO
    fn capacity(&self) -> usize;
    /// TODO
    fn preferred_capacity() -> usize;
    /// TODO
    fn reserve(&mut self, additional: usize);
}

impl<T: Clone + 'static> Container for Vec<T> {
    type ItemRef<'a> = &'a T where T: 'a;
    type Item<'a> = T where T: 'a;

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn clear(&mut self) { Vec::clear(self) }

    type Iter<'a> = std::slice::Iter<'a, T>;

    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        self.as_slice().iter()
    }

    type IntoIter<'a> = <Vec<T> as IntoIterator>::IntoIter;

    fn into_iter<'a>(&'a mut self) -> Self::IntoIter<'a> {
        IntoIterator::into_iter(std::mem::take(self))
    }
}

impl<T> PushContainer for Vec<T> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn preferred_capacity() -> usize {
        buffer::default_capacity::<T>()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional);
    }
}

impl<T> PushInto<Vec<T>> for T {
    fn push_into(self, target: &mut Vec<T>) {
        target.push(self)
    }
}

mod rc {
    use std::ops::Deref;
    use std::rc::Rc;

    use crate::Container;

    impl<T: Container> Container for Rc<T> {
        type ItemRef<'a> = T::ItemRef<'a> where Self: 'a;
        type Item<'a> = T::ItemRef<'a> where Self: 'a;

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }

        fn clear(&mut self) {
            // Try to reuse the allocation if possible
            if let Some(inner) = Rc::get_mut(self) {
                inner.clear();
            } else {
                *self = Self::default();
            }
        }

        type Iter<'a> = T::Iter<'a>;

        fn iter(&self) -> Self::Iter<'_> {
            self.deref().iter()
        }

        type IntoIter<'a> = T::Iter<'a>;

        fn into_iter(&mut self) -> Self::IntoIter<'_> {
            self.iter()
        }
    }
}

mod arc {
    use std::ops::Deref;
    use std::sync::Arc;

    use crate::Container;

    impl<T: Container> Container for Arc<T> {
        type ItemRef<'a> = T::ItemRef<'a> where Self: 'a;
        type Item<'a> = T::ItemRef<'a> where Self: 'a;

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }

        fn clear(&mut self) {
            // Try to reuse the allocation if possible
            if let Some(inner) = Arc::get_mut(self) {
                inner.clear();
            } else {
                *self = Self::default();
            }
        }

        type Iter<'a> = T::Iter<'a>;

        fn iter(&self) -> Self::Iter<'_> {
            self.deref().iter()
        }

        type IntoIter<'a> = T::Iter<'a>;

        fn into_iter(&mut self) -> Self::IntoIter<'_> {
            self.iter()
        }
    }
}

/// A container that can partition itself into pieces.
pub trait PushPartitioned: Container + PushContainer {
    /// Partition and push this container.
    ///
    /// Drain all elements from `self`, and use the function `index` to determine which `buffer` to
    /// append an element to. Call `flush` with an index and a buffer to send the data downstream.
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], index: I, flush: F)
    where
        for<'a> I: FnMut(&Self::Item<'a>) -> usize,
        F: FnMut(usize, &mut Self);
}

impl<T: Container + PushContainer + 'static> PushPartitioned for T where for<'a> T::Item<'a>: PushInto<T> {
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], mut index: I, mut flush: F)
    where
        for<'a> I: FnMut(&Self::Item<'a>) -> usize,
        F: FnMut(usize, &mut Self),
    {
        let ensure_capacity = |this: &mut Self| {
            let capacity = this.capacity();
            let desired_capacity = Self::preferred_capacity();
            if capacity < desired_capacity {
                this.reserve(desired_capacity - capacity);
            }
        };

        for datum in self.into_iter() {
            let index = index(&datum);
            ensure_capacity(&mut buffers[index]);
            datum.push_into(&mut buffers[index]);
            if buffers[index].len() == buffers[index].capacity() {
                flush(index, &mut buffers[index]);
            }
        }
        self.clear();
    }
}

pub mod buffer {
    //! Functionality related to calculating default buffer sizes

    /// The upper limit for buffers to allocate, size in bytes. [default_capacity] converts
    /// this to size in elements.
    pub const BUFFER_SIZE_BYTES: usize = 1 << 13;

    /// The maximum buffer capacity in elements. Returns a number between [BUFFER_SIZE_BYTES]
    /// and 1, inclusively.
    pub const fn default_capacity<T>() -> usize {
        let size = ::std::mem::size_of::<T>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }
}
