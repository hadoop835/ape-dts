use std::{collections::HashMap, fmt::Debug, fs::File, io::Read, str::FromStr};

use configparser::ini::Ini;

use crate::error::Error;

use super::{
    config_enums::{ConflictPolicyEnum, DbType, ExtractType, ParallelType, SinkType},
    datamarker_config::{DataMarkerConfig, DataMarkerSettingEnum, DataMarkerTypeEnum},
    extractor_config::{ExtractorBasicConfig, ExtractorConfig},
    filter_config::FilterConfig,
    parallelizer_config::ParallelizerConfig,
    pipeline_config::PipelineConfig,
    resumer_config::ResumerConfig,
    router_config::RouterConfig,
    runtime_config::RuntimeConfig,
    sinker_config::{SinkerBasicConfig, SinkerConfig},
};

#[derive(Clone)]
pub struct TaskConfig {
    pub extractor_basic: ExtractorBasicConfig,
    pub extractor: ExtractorConfig,
    pub sinker_basic: SinkerBasicConfig,
    pub sinker: SinkerConfig,
    pub runtime: RuntimeConfig,
    pub parallelizer: ParallelizerConfig,
    pub pipeline: PipelineConfig,
    pub filter: FilterConfig,
    pub router: RouterConfig,
    pub resumer: ResumerConfig,
    pub datamarker: DataMarkerConfig,
}

const EXTRACTOR: &str = "extractor";
const CHECK_LOG_DIR: &str = "check_log_dir";
const SINKER: &str = "sinker";
const DB_TYPE: &str = "db_type";
const URL: &str = "url";
const PARALLELIZER: &str = "parallelizer";
const PIPELINE: &str = "pipeline";
const RUNTIME: &str = "runtime";
const FILTER: &str = "filter";
const ROUTER: &str = "router";
const RESUMER: &str = "resumer";
const DATAMARKER_SECTION: &str = "datamarker";
const BATCH_SIZE: &str = "batch_size";

impl TaskConfig {
    pub fn new(task_config_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(task_config_file)
            .unwrap()
            .read_to_string(&mut config_str)
            .unwrap();
        let mut ini = Ini::new();
        ini.read(config_str).unwrap();

        let (extractor_basic, extractor) = Self::load_extractor_config(&ini).unwrap();
        let (sinker_basic, sinker) = Self::load_sinker_config(&ini).unwrap();
        Self {
            extractor_basic,
            extractor,
            parallelizer: Self::load_paralleizer_config(&ini),
            pipeline: Self::load_pipeline_config(&ini),
            sinker_basic,
            sinker,
            runtime: Self::load_runtime_config(&ini).unwrap(),
            filter: Self::load_filter_config(&ini).unwrap(),
            router: Self::load_router_config(&ini).unwrap(),
            resumer: Self::load_resumer_config(&ini).unwrap(),
            datamarker: Self::load_datamarker_config(&ini),
        }
    }

    fn load_extractor_config(ini: &Ini) -> Result<(ExtractorBasicConfig, ExtractorConfig), Error> {
        let db_type = DbType::from_str(&ini.get(EXTRACTOR, DB_TYPE).unwrap()).unwrap();
        let extract_type =
            ExtractType::from_str(&ini.get(EXTRACTOR, "extract_type").unwrap()).unwrap();
        let url = ini.get(EXTRACTOR, URL).unwrap();
        let basic = ExtractorBasicConfig {
            db_type: db_type.clone(),
            extract_type: extract_type.clone(),
            url: url.clone(),
        };

        let sinker = match db_type {
            DbType::Mysql => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::MysqlSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                },

                ExtractType::Cdc => ExtractorConfig::MysqlCdc {
                    url,
                    binlog_filename: ini.get(EXTRACTOR, "binlog_filename").unwrap(),
                    binlog_position: ini.getuint(EXTRACTOR, "binlog_position").unwrap().unwrap()
                        as u32,
                    server_id: ini.getuint(EXTRACTOR, "server_id").unwrap().unwrap(),
                },

                ExtractType::CheckLog => ExtractorConfig::MysqlCheck {
                    url,
                    check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                    batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                },

                ExtractType::Struct => ExtractorConfig::MysqlStruct {
                    url,
                    db: String::new(),
                },
            },

            DbType::Pg => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::PgSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                },

                ExtractType::Cdc => ExtractorConfig::PgCdc {
                    url,
                    slot_name: ini.get(EXTRACTOR, "slot_name").unwrap(),
                    start_lsn: ini.get(EXTRACTOR, "start_lsn").unwrap(),
                    heartbeat_interval_secs: ini
                        .getuint(EXTRACTOR, "heartbeat_interval_secs")
                        .unwrap()
                        .unwrap(),
                },

                ExtractType::CheckLog => ExtractorConfig::PgCheck {
                    url,
                    check_log_dir: ini.get(EXTRACTOR, CHECK_LOG_DIR).unwrap(),
                    batch_size: ini.getuint(EXTRACTOR, BATCH_SIZE).unwrap().unwrap() as usize,
                },

                ExtractType::Struct => ExtractorConfig::PgStruct {
                    url,
                    db: String::new(),
                },
            },

            DbType::Mongo => match extract_type {
                ExtractType::Snapshot => ExtractorConfig::MongoSnapshot {
                    url,
                    db: String::new(),
                    tb: String::new(),
                },

                ExtractType::Cdc => ExtractorConfig::MongoCdc {
                    url,
                    resume_token: Self::get_value(ini, EXTRACTOR, "resume_token"),
                    start_timestamp: Self::get_value(ini, EXTRACTOR, "start_timestamp"),
                    source: Self::get_value(ini, EXTRACTOR, "source"),
                },

                extract_type => {
                    return Err(Error::ConfigError(format!(
                        "extract type: {} not supported",
                        extract_type
                    )))
                }
            },

            DbType::Redis => {
                let repl_port = ini.getuint(EXTRACTOR, "repl_port").unwrap().unwrap();
                match extract_type {
                    ExtractType::Snapshot => ExtractorConfig::RedisSnapshot { url, repl_port },

                    ExtractType::Cdc => ExtractorConfig::RedisCdc {
                        url,
                        repl_port,
                        run_id: ini.get(EXTRACTOR, "run_id").unwrap(),
                        repl_offset: ini.getuint(EXTRACTOR, "repl_offset").unwrap().unwrap(),
                        heartbeat_interval_secs: ini
                            .getuint(EXTRACTOR, "heartbeat_interval_secs")
                            .unwrap()
                            .unwrap(),
                        heartbeat_key: Self::get_value_with_default(
                            ini,
                            EXTRACTOR,
                            "heartbeat_key",
                            "ape_dts_heartbeat_key".into(),
                        ),
                        now_db_id: ini.getint(EXTRACTOR, "now_db_id").unwrap().unwrap(),
                    },

                    extract_type => {
                        return Err(Error::ConfigError(format!(
                            "extract type: {} not supported",
                            extract_type
                        )))
                    }
                }
            }

            DbType::Kafka => ExtractorConfig::Kafka {
                url,
                group: ini.get(EXTRACTOR, "group").unwrap(),
                topic: ini.get(EXTRACTOR, "topic").unwrap(),
                partition: Self::get_value(ini, EXTRACTOR, "partition"),
                offset: Self::get_value(ini, EXTRACTOR, "offset"),
                ack_interval_secs: Self::get_value(ini, EXTRACTOR, "ack_interval_secs"),
            },

            db_type => {
                return Err(Error::ConfigError(format!(
                    "extractor db type: {} not supported",
                    db_type
                )))
            }
        };
        Ok((basic, sinker))
    }

    fn load_sinker_config(ini: &Ini) -> Result<(SinkerBasicConfig, SinkerConfig), Error> {
        let db_type = DbType::from_str(&ini.get(SINKER, DB_TYPE).unwrap()).unwrap();
        let sink_type = SinkType::from_str(&ini.get(SINKER, "sink_type").unwrap()).unwrap();
        let url = ini.get(SINKER, URL).unwrap();
        let batch_size: usize = Self::get_value(ini, SINKER, BATCH_SIZE);

        let basic = SinkerBasicConfig {
            db_type: db_type.clone(),
            url: url.clone(),
            batch_size,
        };

        let conflict_policy_str: String = Self::get_value(ini, SINKER, "conflict_policy");
        let conflict_policy = ConflictPolicyEnum::from_str(&conflict_policy_str).unwrap();

        let sinker = match db_type {
            DbType::Mysql => match sink_type {
                SinkType::Write => SinkerConfig::Mysql { url, batch_size },

                SinkType::Check => SinkerConfig::MysqlCheck {
                    url,
                    batch_size,
                    check_log_dir: ini.get(SINKER, CHECK_LOG_DIR),
                },

                SinkType::Struct => SinkerConfig::MysqlStruct {
                    url,
                    conflict_policy,
                },
            },

            DbType::Pg => match sink_type {
                SinkType::Write => SinkerConfig::Pg { url, batch_size },

                SinkType::Check => SinkerConfig::PgCheck {
                    url,
                    batch_size,
                    check_log_dir: ini.get(SINKER, CHECK_LOG_DIR),
                },

                SinkType::Struct => SinkerConfig::PgStruct {
                    url,
                    conflict_policy,
                },
            },

            DbType::Mongo => match sink_type {
                SinkType::Write => SinkerConfig::Mongo { url, batch_size },

                db_type => {
                    return Err(Error::ConfigError(format!(
                        "sinker db type: {} not supported",
                        db_type
                    )))
                }
            },

            DbType::Kafka => SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs: ini.getuint(SINKER, "ack_timeout_secs").unwrap().unwrap(),
                required_acks: ini.get(SINKER, "required_acks").unwrap(),
            },

            DbType::OpenFaas => SinkerConfig::OpenFaas {
                url,
                batch_size,
                timeout_secs: ini.getuint(SINKER, "timeout_secs").unwrap().unwrap(),
            },

            DbType::Foxlake => SinkerConfig::Foxlake {
                batch_size,
                bucket: ini.get(SINKER, "bucket").unwrap(),
                access_key: ini.get(SINKER, "access_key").unwrap(),
                secret_key: ini.get(SINKER, "secret_key").unwrap(),
                region: ini.get(SINKER, "region").unwrap(),
                root_dir: ini.get(SINKER, "root_dir").unwrap(),
            },

            DbType::Redis => SinkerConfig::Redis {
                url,
                batch_size,
                method: Self::get_value(ini, SINKER, "method"),
            },
        };
        Ok((basic, sinker))
    }

    fn load_paralleizer_config(ini: &Ini) -> ParallelizerConfig {
        // compatible with older versions, Paralleizer settings are set under the pipeline section
        let parallel_sections: Vec<String> = ini
            .sections()
            .iter()
            .filter(|&s| *s == PARALLELIZER)
            .cloned()
            .collect();

        if parallel_sections.is_empty() {
            ParallelizerConfig {
                parallel_size: ini.getuint(PIPELINE, "parallel_size").unwrap().unwrap() as usize,
                parallel_type: ParallelType::from_str(&ini.get(PIPELINE, "parallel_type").unwrap())
                    .unwrap(),
            }
        } else {
            ParallelizerConfig {
                parallel_size: ini.getuint(PARALLELIZER, "parallel_size").unwrap().unwrap()
                    as usize,
                parallel_type: ParallelType::from_str(
                    &ini.get(PARALLELIZER, "parallel_type").unwrap(),
                )
                .unwrap(),
            }
        }
    }

    fn load_pipeline_config(ini: &Ini) -> PipelineConfig {
        let buffer_size = ini.getuint(PIPELINE, "buffer_size").unwrap().unwrap() as usize;
        let batch_sink_interval_secs: u64 =
            Self::get_value(ini, PIPELINE, "batch_sink_interval_secs");
        let checkpoint_interval_secs = ini
            .getuint(PIPELINE, "checkpoint_interval_secs")
            .unwrap()
            .unwrap_or(1);
        let wasm_plugin = if let Some(wp) = ini.get(PIPELINE, "wasm_plugin") {
            wp
        } else {
            "".to_string()
        };

        PipelineConfig {
            buffer_size,
            checkpoint_interval_secs,
            batch_sink_interval_secs,
            wasm_plugin,
        }
    }

    fn load_runtime_config(ini: &Ini) -> Result<RuntimeConfig, Error> {
        Ok(RuntimeConfig {
            log_level: ini.get(RUNTIME, "log_level").unwrap(),
            log_dir: ini.get(RUNTIME, "log_dir").unwrap(),
            log4rs_file: ini.get(RUNTIME, "log4rs_file").unwrap(),
        })
    }

    fn load_filter_config(ini: &Ini) -> Result<FilterConfig, Error> {
        Ok(FilterConfig::Rdb {
            do_dbs: ini.get(FILTER, "do_dbs").unwrap(),
            ignore_dbs: ini.get(FILTER, "ignore_dbs").unwrap(),
            do_tbs: ini.get(FILTER, "do_tbs").unwrap(),
            ignore_tbs: ini.get(FILTER, "ignore_tbs").unwrap(),
            do_events: ini.get(FILTER, "do_events").unwrap(),
        })
    }

    fn load_router_config(ini: &Ini) -> Result<RouterConfig, Error> {
        Ok(RouterConfig::Rdb {
            db_map: ini.get(ROUTER, "db_map").unwrap(),
            tb_map: ini.get(ROUTER, "tb_map").unwrap(),
            field_map: ini.get(ROUTER, "field_map").unwrap(),
        })
    }

    fn load_resumer_config(ini: &Ini) -> Result<ResumerConfig, Error> {
        let mut resume_values = HashMap::new();
        if let Some(values) = ini.get_map().unwrap().get(RESUMER) {
            resume_values = values.clone();
        }
        Ok(ResumerConfig { resume_values })
    }

    fn load_datamarker_config(ini: &Ini) -> DataMarkerConfig {
        let datamarker_sections: Vec<String> = ini
            .sections()
            .iter()
            .filter(|&s| *s == DATAMARKER_SECTION)
            .cloned()
            .collect();

        if datamarker_sections.is_empty() {
            return DataMarkerConfig { setting: None };
        }

        let datamarker_type = ini
            .get(DATAMARKER_SECTION, "type")
            .unwrap_or(DataMarkerTypeEnum::Transaction.to_string());
        match DataMarkerTypeEnum::from_str(&datamarker_type) {
            Ok(e) => match e {
                DataMarkerTypeEnum::Transaction => DataMarkerConfig {
                    setting: Some(DataMarkerSettingEnum::Transaction {
                        transaction_db: ini.get(DATAMARKER_SECTION, "transaction_db").unwrap(),
                        transaction_table: ini
                            .get(DATAMARKER_SECTION, "transaction_table")
                            .unwrap(),
                        transaction_express: ini
                            .get(DATAMARKER_SECTION, "transaction_express")
                            .unwrap(),
                        transaction_command: ini
                            .get(DATAMARKER_SECTION, "transaction_command")
                            .unwrap(),
                        white_nodes: ini.get(DATAMARKER_SECTION, "white_nodes").unwrap(),
                        black_nodes: ini.get(DATAMARKER_SECTION, "black_nodes").unwrap(),
                    }),
                },
            },
            _ => DataMarkerConfig { setting: None },
        }
    }

    fn get_value_with_default<T>(ini: &Ini, section: &str, key: &str, default: T) -> T
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        if let Some(value) = ini.get(section, key) {
            return value.parse::<T>().unwrap();
        }
        default
    }

    fn get_value<T>(ini: &Ini, section: &str, key: &str) -> T
    where
        T: Default,
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        return Self::get_value_with_default(ini, section, key, T::default());
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn mock_props(confs: &str) -> DataMarkerConfig {
        let mut inis = Ini::new();
        inis.read(String::from(confs)).unwrap();
        TaskConfig::load_datamarker_config(&inis)
    }

    #[test]
    fn load_datamarker_config_test() {
        let mut conf: &str = "
        [datamarker]
        type=hehe
        ";
        let mut config = mock_props(conf);
        assert!(config.setting.is_none());

        conf = "
        [datamarker]

        transaction_db=ape_dt
        transaction_table=ape_dt_topo1_node1_node2
        transaction_express=ape_dt_(?<topology>.*)_(?<source>.*)_(?<sink>.*)
        transaction_command=update ape_dt_topo1_node1_node2 set n = n + 1
        white_nodes=4,5,6
        black_nodes=1,2,3
        ";
        config = mock_props(conf);

        match config.setting {
            Some(s) => match s {
                DataMarkerSettingEnum::Transaction {
                    transaction_db,
                    transaction_table,
                    transaction_express,
                    transaction_command,
                    white_nodes,
                    black_nodes,
                } => {
                    assert_eq!(transaction_db, "ape_dt");
                    assert_eq!(transaction_table, "ape_dt_topo1_node1_node2");
                    assert_eq!(
                        transaction_express,
                        "ape_dt_(?<topology>.*)_(?<source>.*)_(?<sink>.*)"
                    );
                    assert_eq!(
                        transaction_command,
                        "update ape_dt_topo1_node1_node2 set n = n + 1"
                    );

                    assert_eq!(white_nodes, "4,5,6");
                    assert_eq!(black_nodes, "1,2,3");
                }
            },
            None => assert!(false),
        }
    }
}
