use std::str::FromStr;

use strum::{Display, EnumString, IntoStaticStr};

use crate::error::Error;

#[derive(Clone, Display, EnumString, IntoStaticStr, Debug, PartialEq, Eq)]
pub enum DbType {
    #[strum(serialize = "mysql")]
    Mysql,
    #[strum(serialize = "pg")]
    Pg,
    // #[strum(serialize = "kafka")]
    // Kafka,
    #[strum(serialize = "open_faas")]
    OpenFaas,
    #[strum(serialize = "foxlake")]
    Foxlake,
    #[strum(serialize = "mongo")]
    Mongo,
    #[strum(serialize = "redis")]
    Redis,
    #[strum(serialize = "starrocks")]
    StarRocks,
}

#[derive(Display, EnumString, IntoStaticStr, Debug, Clone)]
pub enum ExtractType {
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "cdc")]
    Cdc,
    #[strum(serialize = "check_log")]
    CheckLog,
    #[strum(serialize = "struct")]
    Struct,
}

#[derive(Display, EnumString, IntoStaticStr)]
pub enum SinkType {
    #[strum(serialize = "write")]
    Write,
    #[strum(serialize = "check")]
    Check,
    #[strum(serialize = "struct")]
    Struct,
}

#[derive(EnumString, IntoStaticStr, Clone, Display)]
pub enum ParallelType {
    #[strum(serialize = "serial")]
    Serial,
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "rdb_partition")]
    RdbPartition,
    #[strum(serialize = "rdb_merge")]
    RdbMerge,
    #[strum(serialize = "rdb_check")]
    RdbCheck,
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "mongo")]
    Mongo,
    #[strum(serialize = "redis")]
    Redis,
}

pub enum RouteType {
    Db,
    Tb,
}

#[derive(Clone, Debug, IntoStaticStr)]
pub enum ConflictPolicyEnum {
    #[strum(serialize = "ignore")]
    Ignore,
    #[strum(serialize = "interrupt")]
    Interrupt,
}

impl FromStr for ConflictPolicyEnum {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "ignore" => Ok(Self::Ignore),
            _ => Ok(Self::Interrupt),
        }
    }
}
