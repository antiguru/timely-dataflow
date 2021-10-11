
use std::rc::Rc;
use std::cell::RefCell;
use std::any::Any;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::fmt::{self, Debug};
use std::convert::TryFrom;
use std::ops::RangeBounds;

use abomonation_derive::Abomonation;
use timely_container::{Container, IntoAllocated, RefOrMut};

pub struct Registry<Id> {
    /// A worker-specific identifier.
    id: Id,
    /// A map from names to typed loggers.
    map: HashMap<String, (Box<dyn Any>, Box<dyn Flush>)>,
    /// An instant common to all logging statements.
    time: Instant,
}

#[derive(Abomonation, Debug)]
pub struct LogContainer<T, Id> {
    pub time: Duration,
    pub worker: Id,
    pub entries: Vec<(u32, T)>,
}

impl<T, Id: Clone> LogContainer<T, Id> {
    pub fn new(time: Duration, worker: Id) -> Self {
        let entries = Vec::with_capacity(Self::buffer_capacity());
        Self { time, worker, entries }
    }

    fn buffer_capacity() -> usize {
        timely_container::buffer::default_capacity::<(u32, T)>()
    }

    pub fn push<F: FnMut(&mut Self)>(&mut self, time: Duration, data: T, mut overflow_action: F) {
        if time < self.time
            || self.time + Duration::from_nanos(u32::MAX as u64) < time
            || self.entries.len() == self.entries.capacity() {
            // provided time outside permissible range or full
            overflow_action(self);
        }
        self.time = time;
        let offset = u32::try_from(time.as_nanos() - self.time.as_nanos()).unwrap();
        self.entries.push((offset, data));
    }

    pub fn take(&mut self) -> Self {
        let entries = ::std::mem::take(&mut self.entries);
        self.entries = Vec::with_capacity(Self::buffer_capacity());
        Self {
            time: self.time,
            worker: self.worker.clone(),
            entries,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item=(Duration, Id, &T)> {
        let time = self.time;
        let worker = self.worker.clone();
        self.entries.iter().map(move |(offset, t)| (time + Duration::from_nanos(*offset as u64), worker.clone(), t))
    }

    pub fn drain<'a, R: RangeBounds<usize>>(&'a mut self, range: R) -> impl Iterator<Item=(Duration, Id, T)> + 'a {
        let time = self.time;
        let worker = self.worker.clone();
        self.entries.drain(range).map(move |(offset, t)| (time + Duration::from_nanos(offset as u64), worker.clone(), t))
    }
}

impl<Id: Clone+'static> Registry<Id> {
    /// Binds a log name to an action on log event batches.
    ///
    /// This method also returns any pre-installed action, rather than overwriting it
    /// and pivoting the logging destination mid-stream. New loggers with this name will
    /// use the new destination, and existing loggers will use the old destination.
    ///
    /// The action should respond to a sequence of events with non-decreasing timestamps
    /// (Durations) and well as a timestamp that lower bounds the next event that could be
    /// seen (likely greater or equal to the timestamp of the last event). The end of a
    /// logging stream is indicated only by dropping the associated action, which can be
    /// accomplished with `remove` (or a call to insert, though this is not recommended).
    pub fn insert<T: 'static, F: FnMut(&Duration, &mut LogContainer<T, Id>)+'static>(
        &mut self,
        name: &str,
        action: F) -> Option<Box<dyn Any>>
    {
        let logger = Logger::<T, Id>::new(self.time, Duration::default(), self.id.clone(), action);
        self.insert_logger(name, logger)
    }

    /// Binds a log name to a logger.
    pub fn insert_logger<T: 'static>(
        &mut self,
        name: &str,
        logger: Logger<T, Id>) -> Option<Box<dyn Any>>
    {
        self.map.insert(name.to_owned(), (Box::new(logger.clone()), Box::new(logger))).map(|x| x.0)
    }

    /// Removes a bound logger.
    ///
    /// This is intended primarily to close a logging stream and let the associated writer
    /// communicate that the stream is closed to any consumers. If a binding is not removed,
    /// then the stream cannot be complete as in principle anyone could acquire a handle to
    /// the logger and start further logging.
    pub fn remove(&mut self, name: &str) -> Option<Box<dyn Any>> {
        self.map.remove(name).map(|x| x.0)
    }

    /// Retrieves a shared logger, if one has been inserted.
    pub fn get<T: 'static>(&self, name: &str) -> Option<Logger<T, Id>> {
        self.map
            .get(name)
            .and_then(|entry| entry.0.downcast_ref::<Logger<T, Id>>())
            .map(|x| (*x).clone())
    }

    /// Creates a new logger registry.
    pub fn new(time: Instant, id: Id) -> Self {
        Registry {
            id,
            time,
            map: HashMap::new(),
        }
    }

    /// Flushes all registered logs.
    pub fn flush(&mut self) {
        <Self as Flush>::flush(self);
    }
}

impl<Id> Flush for Registry<Id> {
    fn flush(&mut self) {
        for value in self.map.values_mut() {
            value.1.flush();
        }
    }
}

/// A buffering logger.
#[derive(Debug)]
pub struct Logger<T, E: Clone> {
    inner: Rc<RefCell<LoggerInner<T, E, dyn FnMut(&Duration, &mut LogContainer<T, E>)>>>,
}

impl<T, E: Clone> Clone for Logger<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

struct LoggerInner<T, E, A: ?Sized + FnMut(&Duration, &mut LogContainer<T, E>)> {
    id:     E,
    /// common instant used for all loggers.
    time:   Instant,
    /// offset to allow re-calibration.
    offset: Duration,
    /// shared buffer of accumulated log events
    buffer: LogContainer<T, E>,
    /// action to take on full log buffers.
    action: A,
}

impl<T, E: Clone> Logger<T, E> {
    /// Allocates a new shareable logger bound to a write destination.
    pub fn new<F>(time: Instant, offset: Duration, id: E, action: F) -> Self
    where
        F: FnMut(&Duration, &mut LogContainer<T, E>)+'static
    {
        let inner = LoggerInner {
            id: id.clone(),
            time,
            offset,
            action,
            buffer: LogContainer::new(Duration::from_secs(0), id),
        };
        let inner = Rc::new(RefCell::new(inner));
        Logger { inner }
    }

    /// Logs an event.
    ///
    /// The event has its timestamp recorded at the moment of logging, but it may be delayed
    /// due to buffering. It will be written when the logger is next flushed, either due to
    /// the buffer reaching capacity or a direct call to flush.
    ///
    /// This implementation borrows a shared (but thread-local) buffer of log events, to ensure
    /// that the `action` only sees one stream of events with increasing timestamps. This may
    /// have a cost that we don't entirely understand.
    pub fn log<S: Into<T>>(&self, event: S) {
        self.log_many(Some(event));
    }

    /// Logs multiple events.
    ///
    /// The event has its timestamp recorded at the moment of logging, but it may be delayed
    /// due to buffering. It will be written when the logger is next flushed, either due to
    /// the buffer reaching capacity or a direct call to flush.
    ///
    /// All events in this call will have the same timestamp. This can be more performant due
    /// to fewer `time.elapsed()` calls, but it also allows some logged events to appear to be
    /// "transactional", occurring at the same moment.
    ///
    /// This implementation borrows a shared (but thread-local) buffer of log events, to ensure
    /// that the `action` only sees one stream of events with increasing timestamps. This may
    /// have a cost that we don't entirely understand.
    pub fn log_many<I>(&self, events: I)
    where I: IntoIterator, I::Item: Into<T>
    {
        self.inner.borrow_mut().log_many(events)
    }

    /// Flushes logged messages and communicates the new minimal timestamp.
    pub fn flush(&mut self) {
        <Self as Flush>::flush(self);
    }
}

impl<T, E: Clone, A: ?Sized + FnMut(&Duration, &mut LogContainer<T, E>)> LoggerInner<T, E, A> {

    pub fn log_many<I>(&mut self, events: I)
        where I: IntoIterator, I::Item: Into<T>
    {
        let elapsed = self.time.elapsed() + self.offset;

        if self.buffer.entries.is_empty() {
            self.buffer.time = elapsed;
        }

        for event in events {
            let action = &mut self.action;
            self.buffer.push(elapsed, event.into(), |buffer| {
                (action)(&elapsed, buffer);
                // The buffer clear could plausibly be removed, changing the semantics but allowing users
                // to do in-place updates without forcing them to take ownership.
                buffer.entries.clear();
                let buffer_capacity = buffer.entries.capacity();
                if buffer_capacity < LogContainer::<T, E>::buffer_capacity() {
                    buffer.entries.reserve((buffer_capacity+1).next_power_of_two());
                }
            });
        }
    }
}

/// Bit weird, because we only have to flush on the *last* drop, but this should be ok.
impl<T, E: Clone> Drop for Logger<T, E> {
    fn drop(&mut self) {
        // Avoid sending out empty buffers just because of drops.
        if !self.inner.borrow().buffer.entries.is_empty() {
            self.flush();
        }
    }
}

impl<T, E, A: ?Sized + FnMut(&Duration, &mut LogContainer<T, E>)> Debug for LoggerInner<T, E, A>
where
    E: Debug,
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggerInner")
            .field("id", &self.id)
            .field("time", &self.time)
            .field("offset", &self.offset)
            .field("action", &"FnMut")
            .field("buffer", &self.buffer.entries)
            .finish()
    }
}

/// Types that can be flushed.
trait Flush {
    /// Flushes buffered data.
    fn flush(&mut self);
}

impl<T, E: Clone> Flush for Logger<T, E> {
    fn flush(&mut self) {
        self.inner.borrow_mut().flush()
    }
}

impl<T, E: Clone, A: ?Sized + FnMut(&Duration, &mut LogContainer<T, E>)> Flush for LoggerInner<T, E, A> {
    fn flush(&mut self) {
        let elapsed = self.time.elapsed() + self.offset;
        if !self.buffer.entries.is_empty() {
            (self.action)(&elapsed, &mut self.buffer);
            self.buffer.entries.clear();
            // NB: This does not re-allocate any specific size if the buffer has been
            // taken. The intent is that the geometric growth in `log_many` should be
            // enough to ensure that we do not send too many small buffers, nor do we
            // allocate too large buffers when they are not needed.
        }
        else {
            // Avoid swapping resources for empty buffers.
            (self.action)(&elapsed, &mut LogContainer {
                time: elapsed,
                worker: self.id.clone(),
                entries: Vec::new(),
            });
        }
    }
}

impl<T: Clone+'static, E: Container> Container for LogContainer<T, E> {
    type Allocation = Vec<(u32, T)>;

    fn hollow(mut self) -> Self::Allocation {
        self.entries.clear();
        self.entries
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn ensure_capacity(&mut self) {
        self.entries.ensure_capacity();
    }
}

impl<T: Clone+'static, E: Container> IntoAllocated<LogContainer<T, E>> for Vec<(u32, T)> {
    fn assemble(self, allocated: RefOrMut<LogContainer<T, E>>) -> LogContainer<T, E> where Self: Sized {
        match allocated {
            RefOrMut::Mut(r) => LogContainer {
                time: <Duration as Container>::Allocation::assemble_new(RefOrMut::Mut(&mut r.time)),
                worker: <E as Container>::Allocation::assemble_new(RefOrMut::Mut(&mut r.worker)),
                entries: self.assemble(RefOrMut::Mut(&mut r.entries)),
            },
            RefOrMut::Ref(r) => LogContainer {
                time: <Duration as Container>::Allocation::assemble_new(RefOrMut::Ref(&r.time)),
                worker: E::Allocation::assemble_new(RefOrMut::Ref(&r.worker)),
                entries: self.assemble(RefOrMut::Ref(&r.entries)),
            }
        }
    }

    fn assemble_new(allocated: RefOrMut<LogContainer<T, E>>) -> LogContainer<T, E> {
        match allocated {
            RefOrMut::Mut(r) => LogContainer {
                time: <Duration as Container>::Allocation::assemble_new(RefOrMut::Mut(&mut r.time)),
                worker: <E as Container>::Allocation::assemble_new(RefOrMut::Mut(&mut r.worker)),
                entries: <Vec<(u32, T)> as Container>::Allocation::assemble_new(RefOrMut::Mut(&mut r.entries)),
            },
            RefOrMut::Ref(r) => LogContainer {
                time: <Duration as Container>::Allocation::assemble_new(RefOrMut::Ref(&r.time)),
                worker: E::Allocation::assemble_new(RefOrMut::Ref(&r.worker)),
                entries: <Vec<(u32, T)> as Container>::Allocation::assemble_new(RefOrMut::Ref(&r.entries)),
            }
        }
    }
}
