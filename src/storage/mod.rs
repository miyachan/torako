use std::pin::Pin;

pub mod asagi;
pub mod search_pg;

pub trait MetricsProvider: Sync + Send {
    fn name(&self) -> &'static str;
    fn metrics(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Box<dyn erased_serde::Serialize + Send>> + Send>>;
}
