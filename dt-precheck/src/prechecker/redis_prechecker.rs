use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{config_enums::DbType, extractor_config::ExtractorConfig, task_config::TaskConfig},
    error::Error,
    utils::rdb_filter::RdbFilter,
};
use dt_connector::extractor::redis::{
    redis_client::RedisClient, redis_psync_extractor::RedisPsyncExtractor,
};

use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{redis::redis_fetcher::RedisFetcher, traits::Fetcher},
    meta::{check_item::CheckItem, check_result::CheckResult},
};

use super::traits::Prechecker;

pub struct RedisPrechecker {
    pub fetcher: RedisFetcher,
    pub task_config: TaskConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
}

const MIN_SUPPORTED_VERSION: f32 = 2.8;

#[async_trait]
impl Prechecker for RedisPrechecker {
    async fn build_connection(&mut self) -> Result<CheckResult, Error> {
        self.fetcher.build_connection().await?;
        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            DbType::Redis,
            None,
        ))
    }

    async fn check_database_version(&mut self) -> Result<CheckResult, Error> {
        let version = self.fetcher.fetch_version().await?;
        let version: f32 = version.parse().unwrap();
        let check_error = if version < MIN_SUPPORTED_VERSION {
            Some(Error::PreCheckError(format!(
                "redis version:[{}] is NOT supported, the minimum supported version is {}.",
                version, MIN_SUPPORTED_VERSION
            )))
        } else {
            None
        };

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            DbType::Redis,
            check_error,
        ))
    }

    async fn check_cdc_supported(&mut self) -> Result<CheckResult, Error> {
        let repl_port = match self.task_config.extractor {
            ExtractorConfig::RedisCdc { repl_port, .. }
            | ExtractorConfig::RedisSnapshot { repl_port, .. } => repl_port,
            // should never happen since we've already checked the extractor type before into this function
            _ => 0,
        };
        let mut conn = RedisClient::new(&self.fetcher.url).await?;
        let buffer = Arc::new(ConcurrentQueue::bounded(1));

        let filter = RdbFilter::from_config(&self.task_config.filter, DbType::Redis)?;
        let mut psyncer = RedisPsyncExtractor {
            conn: &mut conn,
            run_id: String::new(),
            repl_offset: 0,
            now_db_id: 0,
            repl_port,
            buffer,
            filter,
        };

        if let Err(error) = psyncer.start_psync().await {
            conn.close().await?;
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckAccountPermission,
                self.is_source,
                DbType::Redis,
                Some(error),
            ));
        } else {
            conn.close().await?;
            Ok(CheckResult::build(
                CheckItem::CheckAccountPermission,
                self.is_source,
            ))
        }
    }

    async fn check_permission(&mut self) -> Result<CheckResult, Error> {
        Ok(CheckResult::build(
            CheckItem::CheckAccountPermission,
            self.is_source,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> Result<CheckResult, Error> {
        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            DbType::Redis,
            None,
        ))
    }

    async fn check_table_structs(&mut self) -> Result<CheckResult, Error> {
        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            DbType::Redis,
            None,
        ))
    }
}
