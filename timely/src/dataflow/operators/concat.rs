//! Merges the contents of multiple streams.


use crate::{Container, Data};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{OwnedStream, StreamLike, Scope};

/// Merge the contents of two streams.
pub trait Concat<G: Scope, D: Container, S: StreamLike<G, D>> {
    /// Merge the contents of two streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Concat, Inspect};
    ///
    /// timely::example(|scope| {
    ///
    ///     let stream = (0..10).to_stream(scope).tee();
    ///     stream.concat(&stream)
    ///           .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concat(self, other: S) -> OwnedStream<G, D>;
}

impl<G: Scope, D: Container+Data, S: StreamLike<G, D>> Concat<G, D, S> for S {
    fn concat(self, other: S) -> OwnedStream<G, D> {
        self.scope().concatenate([self, other])
    }
}

/// Merge the contents of multiple streams.
pub trait Concatenate<G: Scope, D: Container, S: StreamLike<G, D>> {
    /// Merge the contents of multiple streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Concatenate, Inspect};
    ///
    /// timely::example(|scope| {
    ///
    ///     let streams = vec![(0..10).to_stream(scope),
    ///                        (0..10).to_stream(scope),
    ///                        (0..10).to_stream(scope)];
    ///
    ///     scope.concatenate(streams)
    ///          .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concatenate<I>(self, sources: I) -> OwnedStream<G, D>
    where
        I: IntoIterator<Item=S>;
}

impl<G: Scope, D: Container+Data> Concatenate<G, D, OwnedStream<G, D>> for OwnedStream<G, D> {
    fn concatenate<I>(self, sources: I) -> OwnedStream<G, D>
    where
        I: IntoIterator<Item=OwnedStream<G, D>>,
    {
        self.scope().concatenate(Some(self).into_iter().chain(sources))
    }
}

impl<G: Scope, D: Container+Data, S: StreamLike<G, D>> Concatenate<G, D, S> for &G {
    fn concatenate<I>(self, sources: I) -> OwnedStream<G, D>
    where
        I: IntoIterator<Item=S>
    {

        // create an operator builder.
        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
        let mut builder = OperatorBuilder::new("Concatenate".to_string(), self.clone());

        // create new input handles for each input stream.
        let mut handles = sources.into_iter().map(|s| builder.new_input(s, Pipeline)).collect::<Vec<_>>();

        // create one output handle for the concatenated results.
        let (mut output, result) = builder.new_output();

        // build an operator that plays out all input data.
        builder.build(move |_capability| {

            let mut vector = Default::default();
            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        data.swap(&mut vector);
                        output.session(&time).give_container(&mut vector);
                    })
                }
            }
        });

        result
    }
}
