use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Position {
    None,
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    RdbSnapshot {
        order_col: String,
        value: String,
    },
    MysqlCdc {
        server_id: String,
        binlog_filename: String,
        next_event_position: u32,
        timestamp: String,
    },
    PgCdc {
        lsn: String,
        timestamp: String,
    },
    MongoCdc {
        resume_token: String,
        operation_time: u32,
        timestamp: String,
    },
    Redis {
        run_id: String,
        repl_offset: u64,
        now_db_id: i64,
        timestamp: String,
    },
}

impl Position {
    pub fn to_string(&self) -> String {
        json!(self).to_string()
    }

    pub fn from_str(str: &str) -> Position {
        serde_json::from_str(str).unwrap()
    }

    pub fn format_timestamp_millis(millis: i64) -> String {
        let naive_datetime = NaiveDateTime::from_timestamp_millis(millis);
        naive_datetime
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S%.3f UTC-0000")
            .to_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_format_timestamp() {
        assert_eq!(
            "2023-03-28 07:33:48.396 UTC-0000",
            Position::format_timestamp_millis(733304028396543 / 1000 + 946_684_800 * 1000)
        );

        assert_eq!(
            "2023-03-28 05:33:47.000 UTC-0000",
            Position::format_timestamp_millis(1679981627 * 1000)
        );
    }
}
