extern crate timely;

use std::net::TcpListener;
use std::time::Duration;

use timely::dataflow::operators::inspect::InspectCore;
use timely::dataflow::operators::capture::{EventReaderCore, Replay};
use timely::logging::{TimelySetup, TimelyEvent};
use timely::logging_core::LogContainer;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

        // create replayers from disjoint partition of source worker identifiers.
        let replayers =
        (0 .. source_peers)
            .filter(|i| i % worker.peers() == worker.index())
            .map(|i| TcpListener::bind(format!("127.0.0.1:{}", 8000 + i)).unwrap())
            .collect::<Vec<_>>()
            .into_iter()
            .map(|l| l.incoming().next().unwrap().unwrap())
            .map(|r| EventReaderCore::<Duration,LogContainer<TimelyEvent,TimelySetup>,_>::new(r))
            .collect::<Vec<_>>();

        worker.dataflow(|scope| {
            replayers
                .replay_into(scope)
                .inspect_container(|x| println!("replayed: {:?}", x));
        })
    }).unwrap(); // asserts error-free execution
}
