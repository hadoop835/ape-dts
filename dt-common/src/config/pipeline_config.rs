#[derive(Clone)]
pub struct PipelineConfig {
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub wasm_plugin: String,
}
