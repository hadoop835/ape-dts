use async_trait::async_trait;

use crate::{call_batch_fn, Sinker};

use dt_common::error::Error;

use dt_meta::{ddl_data::DdlData, row_data::RowData};

use kafka::producer::{Producer, Record};

use super::kafka_router::KafkaRouter;

pub struct KafkaSinker {
    pub batch_size: usize,
    pub kafka_router: KafkaRouter,
    pub producer: Producer,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        call_batch_fn!(self, data, Self::send);
        Ok(())
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl KafkaSinker {
    async fn send(
        &mut self,
        data: &mut [RowData],
        sinked_count: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let mut topics = Vec::new();
        for rd in data.iter().skip(sinked_count).take(batch_size) {
            let topic = self.kafka_router.get_route(&rd.schema, &rd.tb);
            topics.push(topic);
        }

        let mut messages = Vec::new();
        for (i, rd) in data.iter().skip(sinked_count).take(batch_size).enumerate() {
            messages.push(Record {
                key: (),
                value: rd.to_string(),
                topic: &topics[i - sinked_count],
                partition: -1,
            });
        }

        self.producer.send_all(&messages).unwrap();
        Ok(())
    }
}