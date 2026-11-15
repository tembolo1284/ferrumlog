//! A load generator for ferrumlog.
//!
//! Two benchmarks, both driven through the ordinary client library, so
//! they measure what an application would see:
//!
//! * [`produce`] runs a number of workers that each write batches of
//!   records as fast as the cluster acknowledges them, or at a fixed rate.
//! * [`consume`] runs a number of workers that each read a topic from its
//!   first record to its end.
//!
//! Each returns a [`Summary`] with throughput and a latency
//! [`Histogram`].
//!
//! # Reading the latency numbers
//!
//! By default the workers are a closed loop. Each sends its next request
//! when the previous one is answered, and latency is the time one request
//! took. That is the right way to find the highest throughput, but it
//! hides stalls: while the server is stuck, the workers are waiting and
//! send nothing, so one slow request is recorded where a steady stream of
//! clients would have seen many.
//!
//! With a target rate the workers send on a fixed schedule instead, and
//! latency is measured from the moment a request was due, not the moment
//! it was sent. A stall then shows up in every request that had to wait
//! behind it. Use a rate when the percentiles matter.

mod consume;
mod histogram;
mod produce;
mod report;

pub use consume::{consume, ConsumeOptions};
pub use histogram::Histogram;
pub use produce::{produce, Limit, ProduceOptions};
pub use report::Summary;
