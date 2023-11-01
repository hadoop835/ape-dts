use std::{str::FromStr, sync::Arc, time::Duration};

use dt_common::{
    config::{
        config_enums::{ConflictPolicyEnum, DbType},
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
};
use dt_connector::{
    sinker::{
        foxlake_sinker::FoxlakeSinker,
        kafka::{kafka_router::KafkaRouter, kafka_sinker::KafkaSinker},
        mongo::mongo_sinker::MongoSinker,
        mysql::{
            mysql_checker::MysqlChecker, mysql_sinker::MysqlSinker,
            mysql_struct_sinker::MysqlStructSinker,
        },
        open_faas_sinker::OpenFaasSinker,
        pg::{pg_checker::PgChecker, pg_sinker::PgSinker, pg_struct_sinker::PgStructSinker},
        rdb_router::RdbRouter,
        redis::redis_sinker::RedisSinker,
    },
    Sinker,
};
use dt_meta::{
    avro::avro_converter::AvroConverter, mysql::mysql_meta_manager::MysqlMetaManager,
    pg::pg_meta_manager::PgMetaManager, rdb_meta_manager::RdbMetaManager,
    redis::redis_write_method::RedisWriteMethod,
};
use kafka::producer::{Producer, RequiredAcks};
use reqwest::Client;
use rusoto_core::Region;
use rusoto_s3::S3Client;

use super::task_util::TaskUtil;

pub struct SinkerUtil {}

impl SinkerUtil {
    pub async fn create_sinkers(
        task_config: &TaskConfig,
        transaction_command: String,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let sinkers = match &task_config.sinker {
            SinkerConfig::Mysql { url, batch_size } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_mysql_sinker(
                    url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    transaction_command,
                )
                .await?
            }

            SinkerConfig::MysqlCheck {
                url, batch_size, ..
            } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_mysql_checker(
                    url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::Pg { url, batch_size } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_pg_sinker(
                    url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::PgCheck {
                url, batch_size, ..
            } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_pg_checker(
                    url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::Mongo { url, batch_size } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_mongo_sinker(
                    url,
                    &router,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs,
                required_acks,
            } => {
                let router = KafkaRouter::from_config(&task_config.router)?;
                // kafka sinker may need meta data from RDB extractor
                let extractor_url = &task_config.extractor_basic.url;
                let meta_manager = match task_config.extractor_basic.db_type {
                    DbType::Mysql => {
                        let conn_pool =
                            TaskUtil::create_mysql_conn_pool(extractor_url, 1, true).await?;
                        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
                        Some(RdbMetaManager::from_mysql(meta_manager))
                    }
                    DbType::Pg => {
                        let conn_pool =
                            TaskUtil::create_pg_conn_pool(extractor_url, 1, true).await?;
                        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;
                        Some(RdbMetaManager::from_pg(meta_manager))
                    }
                    _ => None,
                };
                let avro_converter = AvroConverter::new(meta_manager);

                SinkerUtil::create_kafka_sinker(
                    url,
                    &router,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    *ack_timeout_secs,
                    required_acks,
                    &avro_converter,
                )
                .await?
            }

            SinkerConfig::OpenFaas {
                url,
                batch_size,
                timeout_secs,
            } => {
                SinkerUtil::create_open_faas_sinker(
                    url,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    *timeout_secs,
                )
                .await?
            }

            SinkerConfig::Foxlake {
                batch_size,
                bucket,
                access_key,
                secret_key,
                region,
                root_dir,
            } => {
                SinkerUtil::create_foxlake_sinker(
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    bucket,
                    root_dir,
                    access_key,
                    secret_key,
                    region,
                )
                .await?
            }

            SinkerConfig::MysqlStruct {
                url,
                conflict_policy,
            } => {
                SinkerUtil::create_mysql_struct_sinker(
                    url,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    conflict_policy,
                )
                .await?
            }

            SinkerConfig::PgStruct {
                url,
                conflict_policy,
            } => {
                SinkerUtil::create_pg_struct_sinker(
                    url,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    conflict_policy,
                )
                .await?
            }

            SinkerConfig::Redis {
                url,
                batch_size,
                method,
            } => {
                SinkerUtil::create_redis_sinker(
                    url,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    method,
                )
                .await?
            }
        };
        Ok(sinkers)
    }

    async fn create_mysql_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
        transaction_command: String,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = MysqlSinker {
                url: url.to_string(),
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
                transaction_command: transaction_command.to_owned(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mysql_checker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = MysqlChecker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_pg_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgSinker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_pg_checker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgChecker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_kafka_sinker<'a>(
        url: &str,
        router: &KafkaRouter,
        parallel_size: usize,
        batch_size: usize,
        ack_timeout_secs: u64,
        required_acks: &str,
        avro_converter: &AvroConverter,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let brokers = vec![url.to_string()];
        let acks = match required_acks {
            "all" => RequiredAcks::All,
            "none" => RequiredAcks::None,
            _ => RequiredAcks::One,
        };

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            // TODO, authentication, https://github.com/kafka-rust/kafka-rust/blob/master/examples/example-ssl.rs
            let producer = Producer::from_hosts(brokers.clone())
                .with_ack_timeout(std::time::Duration::from_secs(ack_timeout_secs))
                .with_required_acks(acks)
                .create()
                .unwrap();
            let sinker = KafkaSinker {
                batch_size,
                kafka_router: router.clone(),
                producer,
                avro_converter: avro_converter.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_open_faas_sinker<'a>(
        url: &str,
        parallel_size: usize,
        batch_size: usize,
        timeout_secs: u64,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let http_client = Client::builder()
                .connect_timeout(Duration::from_secs(timeout_secs))
                .build()
                .unwrap();
            let sinker = OpenFaasSinker {
                batch_size,
                http_client,
                gateway_url: url.to_string(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_foxlake_sinker<'a>(
        parallel_size: usize,
        batch_size: usize,
        bucket: &str,
        root_dir: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let region = Region::from_str(region).unwrap();
            let credentials = rusoto_credential::StaticProvider::new_minimal(
                access_key.to_owned(),
                secret_key.to_owned(),
            );
            let s3_client =
                S3Client::new_with(rusoto_core::HttpClient::new().unwrap(), credentials, region);

            let sinker = FoxlakeSinker {
                batch_size,
                bucket: bucket.to_string(),
                root_dir: root_dir.to_string(),
                s3_client,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mongo_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        parallel_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let mongo_client = TaskUtil::create_mongo_client(url).await.unwrap();
            let sinker = MongoSinker {
                batch_size,
                router: router.clone(),
                mongo_client,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mysql_struct_sinker<'a>(
        url: &str,
        log_level: &str,
        parallel_size: usize,
        conflict_policy: &ConflictPolicyEnum,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = MysqlStructSinker {
                conn_pool: conn_pool.clone(),
                conflict_policy: conflict_policy.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_pg_struct_sinker<'a>(
        url: &str,
        log_level: &str,
        parallel_size: usize,
        conflict_policy: &ConflictPolicyEnum,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgStructSinker {
                conn_pool: conn_pool.clone(),
                conflict_policy: conflict_policy.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_redis_sinker<'a>(
        url: &str,
        parallel_size: usize,
        batch_size: usize,
        method: &str,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let mut conn = TaskUtil::create_redis_conn(url).await?;
            let version = TaskUtil::get_redis_version(&mut conn)?;
            let method = RedisWriteMethod::from_str(method).unwrap();
            let sinker = RedisSinker {
                conn,
                batch_size,
                now_db_id: -1,
                version,
                method,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }
}
