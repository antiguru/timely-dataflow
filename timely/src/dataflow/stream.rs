//! A handle to a typed stream of timely data.
//!
//! Most high-level timely dataflow programming is done with streams, which are each a handle to an
//! operator output. Extension methods on the `Stream` type provide the appearance of higher-level
//! declarative programming, while constructing a dataflow graph underneath.

use crate::progress::{Source, Target};

use crate::communication::Push;
use crate::dataflow::Scope;
use crate::dataflow::channels::pushers::tee::{PushOwned, TeeHelper};
use crate::dataflow::channels::BundleCore;
use std::fmt::{self, Debug};
use crate::Container;
use crate::dataflow::channels::pushers::TeeCore;

/// TODO
pub trait StreamLike<S: Scope, D: Container> {
    /// TODO
    fn connect_to<P: Push<BundleCore<S::Timestamp, D>>+'static>(self, target: Target, pusher: P, identifier: usize);
    /// TODO
    fn scope(&self) -> S;
}

/// Abstraction of a stream of `D: Container` records timestamped with `S::Timestamp`.
///
/// Internally `Stream` maintains a list of data recipients who should be presented with data
/// produced by the source of the stream.
#[derive(Clone)]
pub struct StreamCore<S: Scope, D> {
    /// The progress identifier of the stream's data source.
    name: Source,
    /// The `Scope` containing the stream.
    scope: S,
    /// Maintains a list of Push<Bundle<T, D>> interested in the stream's output.
    ports: TeeHelper<S::Timestamp, D>,
}

/// TODO
pub struct OwnedStream<S: Scope, D> {
    name: Source,
    scope: S,
    port: PushOwned<S::Timestamp, D>,
}

impl<S: Scope, D: Container> OwnedStream<S, D> {
    /// TODO
    pub fn new(name: Source, port: PushOwned<S::Timestamp, D>, scope: S) -> Self {
        Self { name, port, scope }
    }

    /// TODO
    pub fn tee(self) -> StreamCore<S, D> {
        let (target, registrar) = TeeCore::new();
        self.port.set(target);
        StreamCore::new(self.name, registrar, self.scope)
    }
}

/// A stream batching data in vectors.
pub type Stream<S, D> = StreamCore<S, Vec<D>>;

impl<S: Scope, D: Container> StreamLike<S, D> for StreamCore<S, D> {
    fn connect_to<P: Push<BundleCore<S::Timestamp, D>> + 'static>(self, target: Target, pusher: P, identifier: usize) {
        self.connect_to(target, pusher, identifier)
    }

    fn scope(&self) -> S {
        self.scope()
    }
}

impl<S: Scope, D: Container> StreamLike<S, D> for OwnedStream<S, D> {
    fn connect_to<P: Push<BundleCore<S::Timestamp, D>> + 'static>(self, target: Target, pusher: P, identifier: usize) {
        let mut logging = self.scope().logging();
        logging.as_mut().map(|l| l.log(crate::logging::ChannelsEvent {
            id: identifier,
            scope_addr: self.scope.addr(),
            source: (self.name.node, self.name.port),
            target: (target.node, target.port),
        }));

        self.scope.add_edge(self.name, target);
        self.port.set(pusher);
    }

    fn scope(&self) -> S {
        self.scope.clone()
    }
}

impl<S: Scope, D: Container> StreamCore<S, D> {
    /// Connects the stream to a destination.
    ///
    /// The destination is described both by a `Target`, for progress tracking information, and a `P: Push` where the
    /// records should actually be sent. The identifier is unique to the edge and is used only for logging purposes.
    pub fn connect_to<P: Push<BundleCore<S::Timestamp, D>>+'static>(self, target: Target, pusher: P, identifier: usize) {

        let mut logging = self.scope().logging();
        logging.as_mut().map(|l| l.log(crate::logging::ChannelsEvent {
            id: identifier,
            scope_addr: self.scope.addr(),
            source: (self.name.node, self.name.port),
            target: (target.node, target.port),
        }));

        self.scope.add_edge(self.name, target);
        self.ports.add_pusher(pusher);
    }
    /// Allocates a `Stream` from a supplied `Source` name and rendezvous point.
    pub fn new(source: Source, output: TeeHelper<S::Timestamp, D>, scope: S) -> Self {
        Self { name: source, ports: output, scope }
    }
    /// The name of the stream's source operator.
    pub fn name(&self) -> &Source { &self.name }
    /// The scope immediately containing the stream.
    pub fn scope(&self) -> S { self.scope.clone() }
}

impl<S, D> Debug for StreamCore<S, D>
where
    S: Scope,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("source", &self.name)
            // TODO: Use `.finish_non_exhaustive()` after rust/#67364 lands
            .finish()
    }
}
