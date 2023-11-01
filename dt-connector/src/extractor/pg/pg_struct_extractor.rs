use std::sync::{atomic::AtomicBool, Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info, utils::rdb_filter::RdbFilter};

use dt_meta::{
    ddl_data::DdlData,
    ddl_type::DdlType,
    dt_data::{DtData, DtItem},
    position::Position,
    struct_meta::database_model::StructModel,
};

use sqlx::{Pool, Postgres};

use crate::{
    extractor::base_extractor::BaseExtractor, meta_fetcher::pg::pg_struct_fetcher::PgStructFetcher,
    Extractor,
};

pub struct PgStructExtractor {
    pub conn_pool: Pool<Postgres>,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub db: String,
    pub filter: RdbFilter,
    pub shut_down: Arc<AtomicBool>,
}

#[async_trait]
impl Extractor for PgStructExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!("PgStructExtractor starts, schema: {}", self.db,);
        self.extract_internal().await
    }
}

impl PgStructExtractor {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut pg_fetcher = PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            db: self.db.clone(),
            filter: Some(self.filter.to_owned()),
        };

        for (_, seq) in pg_fetcher.get_sequence(&None).await.unwrap() {
            self.push_dt_data(&seq).await;
        }

        for (_, table) in pg_fetcher.get_table(&None).await.unwrap() {
            self.push_dt_data(&table).await;
        }

        for (_, seq_owner) in pg_fetcher.get_sequence_owner(&None).await.unwrap() {
            self.push_dt_data(&seq_owner).await;
        }

        for (_, constraint) in pg_fetcher.get_constraint(&None).await.unwrap() {
            self.push_dt_data(&constraint).await;
        }

        for (_, index) in pg_fetcher.get_index(&None).await.unwrap() {
            self.push_dt_data(&index).await;
        }

        for (_, table_comment) in pg_fetcher.get_table_comment(&None).await.unwrap() {
            self.push_dt_data(&table_comment).await;
        }

        for (_, column_comment) in pg_fetcher.get_column_comment(&None).await.unwrap() {
            self.push_dt_data(&column_comment).await;
        }

        BaseExtractor::wait_task_finish(self.buffer.as_ref(), self.shut_down.as_ref()).await
    }

    pub async fn push_dt_data(&mut self, meta: &StructModel) {
        let ddl_data = DdlData {
            schema: self.db.clone(),
            tb: String::new(),
            query: String::new(),
            meta: Some(meta.to_owned()),
            ddl_type: DdlType::Unknown,
        };
        BaseExtractor::push_dt_data(
            self.buffer.as_ref(),
            DtData::Ddl { ddl_data },
            Position::None,
        )
        .await
        .unwrap()
    }

    pub fn build_fetcher(&self) -> PgStructFetcher {
        PgStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            db: self.db.clone(),
            filter: Some(self.filter.to_owned()),
        }
    }
}
