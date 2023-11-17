use super::config_enums::{ConflictPolicyEnum, DbType};

#[derive(Clone, Debug)]
pub enum SinkerConfig {
    Mysql {
        url: String,
        batch_size: usize,
    },

    Pg {
        url: String,
        batch_size: usize,
    },

    Mongo {
        url: String,
        batch_size: usize,
    },

    MysqlCheck {
        url: String,
        batch_size: usize,
        check_log_dir: Option<String>,
    },

    PgCheck {
        url: String,
        batch_size: usize,
        check_log_dir: Option<String>,
    },

    MysqlStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
    },

    PgStruct {
        url: String,
        conflict_policy: ConflictPolicyEnum,
    },

    // Kafka {
    //     url: String,
    //     batch_size: usize,
    //     ack_timeout_secs: u64,
    //     required_acks: String,
    // },

    OpenFaas {
        url: String,
        batch_size: usize,
        timeout_secs: u64,
    },

    Foxlake {
        batch_size: usize,
        bucket: String,
        access_key: String,
        secret_key: String,
        region: String,
        root_dir: String,
    },

    Redis {
        url: String,
        batch_size: usize,
        method: String,
    },

    Starrocks {
        url: String,
        batch_size: usize,
        stream_load_port: String,
    },
}

#[derive(Clone, Debug)]
pub struct SinkerBasicConfig {
    pub db_type: DbType,
    pub url: String,
    pub batch_size: usize,
}
