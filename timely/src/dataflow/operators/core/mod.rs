//! Extension traits for `Stream` implementing various operators that
//! are independent of specific container types.

pub mod input;
pub mod map;
pub mod ok_err;
pub mod to_stream;
pub mod unordered_input;

pub use input::Input;
pub use map::Map;
pub use ok_err::OkErr;
pub use to_stream::ToStream;
pub use unordered_input::{UnorderedHandle, UnorderedInput};
