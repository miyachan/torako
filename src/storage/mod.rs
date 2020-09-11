pub mod asagi;
pub mod search_pg;

pub trait MetricsProvider: Sync + Send {
    fn name(&self) -> &'static str;
    fn metrics(&self) -> Box<dyn erased_serde::Serialize>;
}
