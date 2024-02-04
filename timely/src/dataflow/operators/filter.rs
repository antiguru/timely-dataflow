//! Filters a stream by a predicate.

use timely_container::columnation::{Columnation, TimelyStack};
use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope, StreamCore};
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter {
    /// The data type we operate on.
    type Data<'a>;
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter<P: 'static>(&self, predicate: P) -> Self
    where
        for<'a> P: FnMut(Self::Data<'a>)->bool;
}

impl<G: Scope, D: Data> Filter for Stream<G, D> {
    type Data<'a> = &'a D;
    fn filter<P: FnMut(&D)->bool+'static>(&self, mut predicate: P) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                vector.retain(|x| predicate(x));
                if !vector.is_empty() {
                    output.session(&time).give_vec(&mut vector);
                }
            });
        })
    }
}

impl<G: Scope, D: Data + Columnation> Filter for StreamCore<G, TimelyStack<D>> {
    type Data<'a> = &'a D;
    fn filter<P: FnMut(&D)->bool+'static>(&self, mut predicate: P) -> StreamCore<G, TimelyStack<D>> {
        let mut vector = Default::default();
        let mut filtered = TimelyStack::default();
        self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                for item in vector.iter().filter(|x| predicate(x)) {
                    filtered.copy(item);
                }
                if !filtered.is_empty() {
                    output.session(&time).give_container(&mut filtered);
                }
            });
        })
    }
}
