use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("config error: {0}")]
    ConfigError(String),

    #[error("extractor error: {0}")]
    ExtractorError(String),

    #[error("sinker error: {0}")]
    SinkerError(String),

    #[error("heartbeat error: {0}")]
    HearbeatError(String),

    #[error("pull mysql binlog error: {0}")]
    BinlogError(#[from] mysql_binlog_connector_rust::binlog_error::BinlogError),

    #[error("sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),

    #[error("unexpected error: {0}")]
    Unexpected(String),

    #[error("parse redis rdb error: {0}")]
    RedisRdbError(String),

    #[error("metadata error: {0}")]
    MetadataError(String),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("yaml error: {0}")]
    YamlError(#[from] serde_yaml::Error),

    #[error("from utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("mongodb error: {0}")]
    MongodbError(#[from] mongodb::error::Error),

    #[error("struct error: {0}")]
    StructError(String),

    #[error("precheck error: {0}")]
    PreCheckError(String),

    #[error("kafka error: {0}")]
    KafkaError(#[from] kafka::Error),

    #[error("udf error: {0}")]
    UdfError(String),

    #[error("avro encode error: {0}")]
    AvroEncodeError(#[from] apache_avro::Error),

    #[error("enum parse error: {0}")]
    EnumParseError(#[from] strum::ParseError),
}
