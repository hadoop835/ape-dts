use std::{
    fs::{self, File},
    io::Read,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{
        config_enums::DbType, datamarker_config::DataMarkerSettingEnum,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    syncer::Syncer,
    utils::rdb_filter::RdbFilter,
};
use dt_connector::{extractor::snapshot_resumer::SnapshotResumer, Extractor};
use dt_meta::{dt_data::DtData, row_type::RowType};
use dt_pipeline::{
    base_pipeline::BasePipeline, udf::wasm::wasm_udf_loader::WasmUdfLoader, Pipeline,
};

use log4rs::config::RawConfig;
use tokio::try_join;

use super::{
    extractor_util::ExtractorUtil, parallelizer_util::ParallelizerUtil, sinker_util::SinkerUtil,
};

pub struct TaskRunner {
    config: TaskConfig,
}

const CHECK_LOG_DIR_PLACEHODLER: &str = "CHECK_LOG_DIR_PLACEHODLER";
const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const DEFAULT_CHECK_LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER/check";

impl TaskRunner {
    pub async fn new(task_config_file: String) -> Self {
        Self {
            config: TaskConfig::new(&task_config_file),
        }
    }

    pub async fn start_task(&self, enable_log4rs: bool) -> Result<(), Error> {
        if enable_log4rs {
            self.init_log4rs()?;
        }

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct { url, .. }
            | ExtractorConfig::PgStruct { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::MongoSnapshot { url, .. } => self.start_multi_task(url).await?,

            _ => self.start_single_task(&self.config.extractor).await?,
        };

        Ok(())
    }

    async fn start_multi_task(&self, url: &str) -> Result<(), Error> {
        let db_type = self.config.extractor.get_db_type();
        let mut filter = RdbFilter::from_config(&self.config.filter, db_type.clone())?;
        let dbs = ExtractorUtil::list_dbs(url, &db_type).await?;
        for db in dbs.iter() {
            if filter.filter_db(db) {
                continue;
            }

            // start a task for each db
            let db_extractor_config = match &self.config.extractor {
                ExtractorConfig::MysqlStruct { url, .. } => Some(ExtractorConfig::MysqlStruct {
                    url: url.clone(),
                    db: db.clone(),
                }),

                ExtractorConfig::PgStruct { url, .. } => Some(ExtractorConfig::PgStruct {
                    url: url.clone(),
                    db: db.clone(),
                }),

                _ => None,
            };

            if let Some(extractor_config) = db_extractor_config {
                self.start_single_task(&extractor_config).await?;
                continue;
            }

            // start a task for each tb
            let tbs = ExtractorUtil::list_tbs(url, db, &db_type).await?;
            for tb in tbs.iter() {
                if filter.filter_event(db, tb, &RowType::Insert.to_string()) {
                    continue;
                }

                let tb_extractor_config = match &self.config.extractor {
                    ExtractorConfig::MysqlSnapshot { url, .. } => ExtractorConfig::MysqlSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
                    },

                    ExtractorConfig::PgSnapshot { url, .. } => ExtractorConfig::PgSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
                    },

                    ExtractorConfig::MongoSnapshot { url, .. } => ExtractorConfig::MongoSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
                    },

                    _ => {
                        return Err(Error::ConfigError("unsupported extractor config".into()));
                    }
                };

                self.start_single_task(&tb_extractor_config).await?;
            }
        }
        Ok(())
    }

    async fn start_single_task(&self, extractor_config: &ExtractorConfig) -> Result<(), Error> {
        let buffer = Arc::new(ConcurrentQueue::bounded(self.config.pipeline.buffer_size));
        let shut_down = Arc::new(AtomicBool::new(false));
        let syncer = Arc::new(Mutex::new(Syncer {
            checkpoint_position: String::new(),
        }));

        let mut extractor = self
            .create_extractor(
                extractor_config,
                buffer.clone(),
                shut_down.clone(),
                syncer.clone(),
            )
            .await?;

        let mut pipeline = self
            .create_pipeline(buffer, shut_down.clone(), syncer)
            .await?;

        let f1 = tokio::spawn(async move {
            extractor.extract().await.unwrap();
            extractor.close().await.unwrap();
        });
        let f2 = tokio::spawn(async move {
            pipeline.start().await.unwrap();
            pipeline.stop().await.unwrap();
        });
        let _ = try_join!(f1, f2);
        Ok(())
    }

    async fn create_pipeline(
        &self,
        buffer: Arc<ConcurrentQueue<DtData>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<Box<dyn Pipeline + Send>, Error> {
        let transaction_command = self.fetch_transaction_command();

        let parallelizer = ParallelizerUtil::create_parallelizer(&self.config).await?;
        let sinkers = SinkerUtil::create_sinkers(&self.config, transaction_command).await?;

        let udf_loader = if self.config.pipeline.wasm_plugin.to_owned().is_empty() {
            None
        } else {
            let mut loader = WasmUdfLoader {
                store: None,
                instance: None,
                wasi_env: None,
            };
            loader.init_instance(self.config.pipeline.wasm_plugin.to_owned());
            Some(loader)
        };

        let pipeline = BasePipeline {
            buffer,
            parallelizer,
            sinker_basic_config: self.config.sinker_basic.clone(),
            sinkers,
            shut_down,
            checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
            batch_sink_interval_secs: self.config.pipeline.batch_sink_interval_secs,
            syncer: syncer.to_owned(),
            udf_loader,
        };

        Ok(Box::new(pipeline))
    }

    async fn create_extractor(
        &self,
        extractor_config: &ExtractorConfig,
        buffer: Arc<ConcurrentQueue<DtData>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<Box<dyn Extractor + Send>, Error> {
        let resumer = SnapshotResumer {
            resumer_values: self.config.resumer.resume_values.clone(),
            db_type: extractor_config.get_db_type(),
        };

        let extractor: Box<dyn Extractor + Send> = match extractor_config {
            ExtractorConfig::MysqlSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_mysql_snapshot_extractor(
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    resumer.clone(),
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let extractor = ExtractorUtil::create_mysql_check_extractor(
                    url,
                    check_log_dir,
                    *batch_size,
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCdc {
                url,
                binlog_filename,
                binlog_position,
                server_id,
            } => {
                let filter = RdbFilter::from_config_with_transaction(
                    &self.config.filter,
                    DbType::Mysql,
                    &self.config.datamarker,
                )?;

                let datamarker_filter = ExtractorUtil::datamarker_filter_builder(
                    &self.config.extractor,
                    &self.config.datamarker,
                )?;

                let extractor = ExtractorUtil::create_mysql_cdc_extractor(
                    url,
                    binlog_filename,
                    *binlog_position,
                    *server_id,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                    datamarker_filter,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_pg_snapshot_extractor(
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    resumer.clone(),
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let extractor = ExtractorUtil::create_pg_check_extractor(
                    url,
                    check_log_dir,
                    *batch_size,
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCdc {
                url,
                slot_name,
                start_lsn,
                heartbeat_interval_secs,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_cdc_extractor(
                    url,
                    slot_name,
                    start_lsn,
                    *heartbeat_interval_secs,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_mongo_snapshot_extractor(
                    url,
                    db,
                    tb,
                    resumer.clone(),
                    buffer,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoCdc {
                url,
                resume_token,
                start_timestamp,
                source,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mongo)?;
                let extractor = ExtractorUtil::create_mongo_cdc_extractor(
                    url,
                    resume_token,
                    start_timestamp,
                    source,
                    buffer,
                    filter,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlStruct { url, db } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mysql)?;
                let extractor = ExtractorUtil::create_mysql_struct_extractor(
                    url,
                    db,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgStruct { url, db } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_struct_extractor(
                    url,
                    db,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshot { url, repl_port } => {
                let extractor = ExtractorUtil::create_redis_snapshot_extractor(
                    url, *repl_port, buffer, shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::RedisCdc {
                url,
                run_id,
                repl_offset,
                now_db_id,
                repl_port,
                heartbeat_interval_secs,
            } => {
                let extractor = ExtractorUtil::create_redis_cdc_extractor(
                    url,
                    run_id,
                    *repl_offset,
                    *repl_port,
                    *now_db_id,
                    *heartbeat_interval_secs,
                    buffer,
                    shut_down,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::Kafka {
                url,
                group,
                topic,
                partition,
                offset,
                ack_interval_secs,
            } => {
                let extractor = ExtractorUtil::create_kafka_extractor(
                    url,
                    group,
                    topic,
                    *partition,
                    *offset,
                    *ack_interval_secs,
                    buffer,
                    shut_down,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }
        };
        Ok(extractor)
    }

    fn fetch_transaction_command(&self) -> String {
        match &self.config.datamarker.setting {
            Some(s) => match s {
                DataMarkerSettingEnum::Transaction {
                    transaction_command,
                    ..
                } => transaction_command.to_owned(),
            },
            None => String::from(""),
        }
    }

    fn init_log4rs(&self) -> Result<(), Error> {
        let log4rs_file = &self.config.runtime.log4rs_file;
        if fs::metadata(log4rs_file).is_err() {
            return Ok(());
        }

        let mut config_str = String::new();
        File::open(log4rs_file)?.read_to_string(&mut config_str)?;

        match &self.config.sinker {
            SinkerConfig::MysqlCheck { check_log_dir, .. }
            | SinkerConfig::PgCheck { check_log_dir, .. } => {
                if let Some(dir) = check_log_dir {
                    if !dir.is_empty() {
                        config_str = config_str.replace(CHECK_LOG_DIR_PLACEHODLER, dir);
                    }
                }
            }
            _ => {}
        }

        config_str = config_str
            .replace(CHECK_LOG_DIR_PLACEHODLER, DEFAULT_CHECK_LOG_DIR_PLACEHODLER)
            .replace(LOG_DIR_PLACEHODLER, &self.config.runtime.log_dir)
            .replace(LOG_LEVEL_PLACEHODLER, &self.config.runtime.log_level);

        let config: RawConfig = serde_yaml::from_str(&config_str)?;
        log4rs::init_raw_config(config).unwrap();
        Ok(())
    }
}
