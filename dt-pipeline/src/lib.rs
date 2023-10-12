pub mod base_pipeline;
pub mod udf;

use async_trait::async_trait;
use dt_common::error::Error;

#[async_trait]
pub trait Pipeline {
    async fn start(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
