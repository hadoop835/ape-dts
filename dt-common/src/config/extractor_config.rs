use super::{
    config_enums::{DbType, ExtractType},
    s3_config::S3Config,
};

#[derive(Clone, Debug)]
pub enum ExtractorConfig {
    MysqlStruct {
        url: String,
        db: String,
    },

    PgStruct {
        url: String,
        schema: String,
    },

    MysqlSnapshot {
        url: String,
        db: String,
        tb: String,
        sample_interval: usize,
    },

    MysqlCdc {
        url: String,
        binlog_filename: String,
        binlog_position: u32,
        server_id: u64,
        gtid_enabled: bool,
        gtid_set: String,
        heartbeat_interval_secs: u64,
        heartbeat_tb: String,
        start_time_utc: String,
        end_time_utc: String,
    },

    MysqlCheck {
        url: String,
        check_log_dir: String,
        batch_size: usize,
    },

    PgSnapshot {
        url: String,
        schema: String,
        tb: String,
        sample_interval: usize,
    },

    PgCdc {
        url: String,
        slot_name: String,
        pub_name: String,
        start_lsn: String,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_tb: String,
        ddl_command_tb: String,
        start_time_utc: String,
        end_time_utc: String,
    },

    PgCheck {
        url: String,
        check_log_dir: String,
        batch_size: usize,
    },

    MongoSnapshot {
        url: String,
        app_name: String,
        db: String,
        tb: String,
    },

    MongoCdc {
        url: String,
        app_name: String,
        resume_token: String,
        start_timestamp: u32,
        // op_log, change_stream
        source: String,
        heartbeat_interval_secs: u64,
        heartbeat_tb: String,
    },

    MongoCheck {
        url: String,
        app_name: String,
        check_log_dir: String,
        batch_size: usize,
    },

    RedisSnapshot {
        url: String,
        repl_port: u64,
    },

    RedisCdc {
        url: String,
        repl_id: String,
        repl_offset: u64,
        repl_port: u64,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_key: String,
        now_db_id: i64,
    },

    RedisSnapshotFile {
        file_path: String,
    },

    RedisScan {
        url: String,
        scan_count: u64,
        statistic_type: String,
    },

    RedisReshard {
        url: String,
        to_node_ids: String,
    },

    Kafka {
        url: String,
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
        ack_interval_secs: u64,
    },

    FoxlakeS3 {
        url: String,
        schema: String,
        tb: String,
        s3_config: S3Config,
    },
}

#[derive(Clone, Debug)]
pub struct BasicExtractorConfig {
    pub db_type: DbType,
    pub extract_type: ExtractType,
    pub url: String,
}
