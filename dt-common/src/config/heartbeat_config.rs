#[derive(Clone)]
pub struct HeartbeatConfig {
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
}
