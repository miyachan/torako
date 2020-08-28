pub mod asagi;

pub trait MetricsProvider: Sync + Send {
    fn name(&self) -> &'static str;
    fn metrics(&self) -> Box<dyn erased_serde::Serialize>;
}
