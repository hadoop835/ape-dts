use super::config_enums::{DbType, ExtractType};

#[derive(Clone, Debug)]
pub enum ExtractorConfig {
    MysqlStruct {
        url: String,
        db: String,
    },

    PgStruct {
        url: String,
        db: String,
    },

    MysqlSnapshot {
        url: String,
        db: String,
        tb: String,
    },

    MysqlCdc {
        url: String,
        binlog_filename: String,
        binlog_position: u32,
        server_id: u64,
    },

    MysqlCheck {
        url: String,
        check_log_dir: String,
        batch_size: usize,
    },

    PgSnapshot {
        url: String,
        db: String,
        tb: String,
    },

    PgCdc {
        url: String,
        slot_name: String,
        start_lsn: String,
        heartbeat_interval_secs: u64,
    },

    PgCheck {
        url: String,
        check_log_dir: String,
        batch_size: usize,
    },

    MongoSnapshot {
        url: String,
        db: String,
        tb: String,
    },

    MongoCdc {
        url: String,
        resume_token: String,
        start_timestamp: u32,
        // op_log, change_stream
        source: String,
    },

    RedisSnapshot {
        url: String,
        repl_port: u64,
    },

    RedisCdc {
        url: String,
        run_id: String,
        repl_offset: u64,
        repl_port: u64,
        heartbeat_interval_secs: u64,
        heartbeat_key: String,
        now_db_id: i64,
    },

    Kafka {
        url: String,
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
        ack_interval_secs: u64,
    },
}

#[derive(Clone, Debug)]
pub struct ExtractorBasicConfig {
    pub db_type: DbType,
    pub extract_type: ExtractType,
    pub url: String,
}

impl ExtractorConfig {
    pub fn get_db_type(&self) -> DbType {
        match self {
            Self::MysqlStruct { .. }
            | Self::MysqlSnapshot { .. }
            | Self::MysqlCdc { .. }
            | Self::MysqlCheck { .. } => DbType::Mysql,
            Self::PgStruct { .. }
            | Self::PgSnapshot { .. }
            | Self::PgCdc { .. }
            | Self::PgCheck { .. } => DbType::Pg,
            Self::MongoSnapshot { .. } | Self::MongoCdc { .. } => DbType::Mongo,
            Self::RedisSnapshot { .. } | Self::RedisCdc { .. } => DbType::Redis,
            Self::Kafka { .. } => DbType::Kafka,
        }
    }

    pub fn get_url(&self) -> String {
        match self {
            Self::MysqlStruct { url, .. }
            | Self::MysqlSnapshot { url, .. }
            | Self::MysqlCdc { url, .. }
            | Self::MysqlCheck { url, .. }
            | Self::PgStruct { url, .. }
            | Self::PgSnapshot { url, .. }
            | Self::PgCdc { url, .. }
            | Self::PgCheck { url, .. }
            | Self::MongoSnapshot { url, .. }
            | Self::MongoCdc { url, .. }
            | Self::RedisSnapshot { url, .. }
            | Self::RedisCdc { url, .. }
            | Self::Kafka { url, .. } => url.to_owned(),
        }
    }
}
