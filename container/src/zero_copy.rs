//! Zero-copy container builders

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use flatcontainer::{FlatStack, Push, Region};
use flatcontainer::flatten::{DefaultFlatWrite, DerefWrapper, Entomb, Exhume};
use crate::{Container, ContainerBuilder, PushInto};

type Buffer = DerefWrapper<Arc<Vec<u8>>>;

/// TODO
pub struct ZeroCopyBuilder<R>
where
    R: Region + Exhume<Buffer>,
{
    pending: FlatStack<R>,
    ready: VecDeque<Vec<u8>>,
    current: Option<ZeroCopyWrapper<FlatStack<R::Flat>>>,
}

impl<R> Default for ZeroCopyBuilder<R>
where
    R: Region + Exhume<Buffer>,
    R::Flat: Region,
{
    fn default() -> Self {
        Self {
            pending: FlatStack::default(),
            ready: VecDeque::default(),
            current: None,
        }
    }
}

impl<R> ContainerBuilder for ZeroCopyBuilder<R>
where
    R: Clone + Default + Entomb + Exhume<Buffer> + Region + 'static,
    R::Flat: Clone + Exhume<DerefWrapper<Arc<Vec<u8>>>> + 'static,
{
    type Container = ZeroCopyWrapper<FlatStack<R::Flat>>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.current = self.ready.pop_front().map(|buffer| {
            let buffer = Arc::new(buffer);
            let length = buffer.len();
            ZeroCopyWrapper {
                buffer,
                length,
                _marker: PhantomData,
            }
        });
        self.current.as_mut()
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.pending.is_empty() {
            let mut length = 0;
            type W<'a> = DefaultFlatWrite<&'a mut Vec<u8>>;
            self.pending.flat_size::<W<'_>>(&mut length);
            W::finish_size(&mut length);
            let mut buffer = Vec::with_capacity(length);
            let mut write = DefaultFlatWrite::new(&mut buffer);
            self.pending.entomb(&mut write).unwrap();
            self.ready.push_back(buffer);
        }

        self.extract()
    }
}

impl<R, T> PushInto<T> for ZeroCopyBuilder<R>
where
    R: Region + Entomb + Exhume<Buffer> + Push<T>,
    R::Flat: Region,
{
    fn push_into(&mut self, item: T) {
        self.pending.copy(item);

        // Estimate `pending` size in bytes
        let mut length = 0;
        type W<'a> = DefaultFlatWrite<&'a mut Vec<u8>>;
        self.pending.flat_size::<W<'_>>(&mut length);
        W::finish_size(&mut length);
        if length > 1024 {
            let mut buffer = Vec::with_capacity(length);
            let mut write = DefaultFlatWrite::new(&mut buffer);
            self.pending.entomb(&mut write).unwrap();
            self.ready.push_back(buffer);
        }
    }
}

/// TODO
pub struct ZeroCopyWrapper<R> {
    buffer: Arc<Vec<u8>>,
    length: usize,
    _marker: PhantomData<R>,
}

impl<R> Clone for ZeroCopyWrapper<R> {
    fn clone(&self) -> Self {
        Self {
            buffer: Arc::clone(&self.buffer),
            length: self.length,
            _marker: PhantomData,
        }
    }
}

impl<R> Default for ZeroCopyWrapper<R> {
    fn default() -> Self {
        Self {
            buffer: Arc::new(Vec::new()),
            length: 0,
            _marker: PhantomData,
        }
    }
}

impl<R> Container for ZeroCopyWrapper<FlatStack<R>>
where
    for<'a> R: Exhume<DerefWrapper<Arc<Vec<u8>>>> + Region +'static,
{
    type ItemRef<'a> = R::ReadItem<'a> where Self: 'a;
    type Item<'a> = R::ReadItem<'a> where Self: 'a;

    fn len(&self) -> usize {
        self.length
    }

    fn clear(&mut self) {
        todo!()
    }

    type Iter<'a> = std::iter::Empty<R::ReadItem<'a>>;

    fn iter(&self) -> Self::Iter<'_> {
        std::iter::empty()
    }

    type DrainIter<'a> = std::iter::Empty<R::ReadItem<'a>>;

    fn drain(&mut self) -> Self::DrainIter<'_> {
        std::iter::empty()
    }
}
