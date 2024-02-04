//! Extension methods for `Stream` based on record-by-record transformation.

use timely_container::columnation::{Columnation, TimelyStack};
use crate::Data;
use crate::dataflow::{Stream, Scope, StreamCore};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;

/// Extension trait for `Stream`.
pub trait Map<S: Scope> {
    /// The type of data to be mapped.
    type Data<'a>;
    /// Consumes each element of the stream and yields a new element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<D2: Data, L: 'static>(&self, logic: L) -> Stream<S, D2>
    where
        for<'a> L: FnMut(Self::Data<'a>)->D2;
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<I: IntoIterator, L: 'static>(&self, logic: L) -> Stream<S, I::Item>
    where
        I::Item: Data,
        for<'a> L: FnMut(Self::Data<'a>)->I;
}

/// Extension trait for `Stream`.
pub trait MapInPlace<S: Scope, D: Data> {
    /// Updates each element of the stream and yields the element, re-using memory where possible.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, MapInPlace, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_in_place(|x| *x += 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_in_place<L: FnMut(&mut D)+'static>(&self, logic: L) -> Stream<S, D>;
}

impl<S: Scope, D: Data + Columnation> Map<S> for StreamCore<S, TimelyStack<D>> {
    type Data<'a> = &'a D;
    fn map<D2: Data, L: 'static>(&self, mut logic: L) -> Stream<S, D2>
    where
        for<'a> L: FnMut(&'a D)->D2,
    {
        let mut vector = Default::default();
        self.unary(Pipeline, "Map", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.iter().map(|x| logic(x)));
            });
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: IntoIterator, L: 'static>(&self, mut logic: L) -> Stream<S, I::Item>
    where
        I::Item: Data,
        for<'a> L: FnMut(&'a D)->I,
    {
        let mut vector = Default::default();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.iter().flat_map(|x| logic(x).into_iter()));
            });
        })
    }
}

impl<S: Scope, D: Data> Map<S> for Stream<S, D> {
    type Data<'a> = D;
    fn map<D2: Data, L: FnMut(D)->D2+'static>(&self, mut logic: L) -> Stream<S, D2> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Map", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain(..).map(|x| logic(x)));
            });
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: IntoIterator, L: FnMut(D)->I+'static>(&self, mut logic: L) -> Stream<S, I::Item> where I::Item: Data {
        let mut vector = Vec::new();
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain(..).flat_map(|x| logic(x).into_iter()));
            });
        })
    }
}

impl<S: Scope, D: Data> MapInPlace<S, D> for Stream<S, D> {
    fn map_in_place<L: FnMut(&mut D) + 'static>(&self, mut logic: L) -> Stream<S, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "MapInPlace", move |_, _| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                for datum in vector.iter_mut() { logic(datum); }
                output.session(&time).give_vec(&mut vector);
            })
        })
    }
}
