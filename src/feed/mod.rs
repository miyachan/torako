/*
 * This implementation of feed is extracted from:
 * https://github.com/rust-lang/futures-rs/pull/2155
 */
use futures::sink::Sink;
use futures::stream::{Stream, TryStream};

mod feed;
pub use self::feed::Feed;

mod feed_all;
pub use self::feed_all::FeedAll;

impl<T: ?Sized, Item> FeedSinkExt<Item> for T where T: Sink<Item> {}

pub trait FeedSinkExt<Item>: Sink<Item> {
    /// A future that completes after the given item has been received
    /// by the sink.
    ///
    /// Unlike `send`, the returned future does not flush the sink.
    /// It is the caller's responsibility to ensure all pending items
    /// are processed, which can be done via `flush` or `close`.
    fn feed(&mut self, item: Item) -> Feed<'_, Self, Item>
    where
        Self: Unpin,
    {
        Feed::new(self, item)
    }

    /// A future that completes after the given stream has been fully received
    /// by the sink.
    ///
    /// This future will drive the stream to keep producing items until it is
    /// exhausted, sending each item to the sink. It will complete once the
    /// stream is exhausted and the sink has received all items.
    /// Note that the sink is **not** closed.
    ///
    /// Unlike `send_all`, the returned future does not fully flush the sink
    /// before completion.
    /// It is the caller's responsibility to ensure all pending items
    /// are processed, which can be done via `flush` or `close`.
    fn feed_all<'a, St>(&'a mut self, stream: &'a mut St) -> FeedAll<'a, Self, St>
    where
        St: TryStream<Ok = Item, Error = Self::Error> + Stream + Unpin + ?Sized,
        Self: Unpin,
    {
        FeedAll::new(self, stream)
    }
}
