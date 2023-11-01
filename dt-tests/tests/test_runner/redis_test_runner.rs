use std::collections::HashMap;

use super::base_test_runner::BaseTestRunner;
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    utils::{rdb_filter::RdbFilter, time_util::TimeUtil},
};
use dt_connector::sinker::redis::cmd_encoder::CmdEncoder;
use dt_meta::redis::redis_object::RedisCmd;
use dt_task::task_util::TaskUtil;
use redis::{Connection, ConnectionLike, Value};

const SRC: &str = "src";
const DST: &str = "dst";

const SYSTEM_KEYS: [&str; 5] = [
    "backup1",
    "backup2",
    "backup3",
    "backup4",
    "ape_dts_heartbeat_key",
];

pub struct RedisTestRunner {
    pub base: BaseTestRunner,
    src_conn: Connection,
    dst_conn: Connection,
    delimiters: Vec<char>,
    escape_pairs: Vec<(char, char)>,
    filter: RdbFilter,
}

impl RedisTestRunner {
    pub async fn new_default(relative_test_dir: &str) -> Result<Self, Error> {
        Self::new(relative_test_dir, vec![' '], vec![('"', '"')]).await
    }

    pub async fn new(
        relative_test_dir: &str,
        delimiters: Vec<char>,
        escape_pairs: Vec<(char, char)>,
    ) -> Result<Self, Error> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        let config = TaskConfig::new(&base.task_config_file);
        let src_conn = match config.extractor {
            ExtractorConfig::RedisSnapshot { url, .. } | ExtractorConfig::RedisCdc { url, .. } => {
                TaskUtil::create_redis_conn(&url).await.unwrap()
            }
            _ => {
                return Err(Error::ConfigError("unsupported extractor config".into()));
            }
        };

        let dst_conn = match config.sinker {
            SinkerConfig::Redis { url, .. } => TaskUtil::create_redis_conn(&url).await.unwrap(),
            _ => {
                return Err(Error::ConfigError("unsupported sinker config".into()));
            }
        };

        let filter = RdbFilter::from_config(&config.filter, DbType::Redis)?;
        Ok(Self {
            base,
            src_conn,
            dst_conn,
            delimiters,
            escape_pairs,
            filter,
        })
    }

    pub async fn run_snapshot_test(&mut self) -> Result<(), Error> {
        self.execute_test_ddl_sqls()?;

        println!("src: {}", TaskUtil::get_redis_version(&mut self.src_conn)?);
        println!("dst: {}", TaskUtil::get_redis_version(&mut self.dst_conn)?);

        self.execute_cmds(SRC, &self.base.src_dml_sqls.clone());
        self.base.start_task().await?;
        self.compare_all_data()
    }

    pub async fn run_cdc_test(
        &mut self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<(), Error> {
        self.execute_test_ddl_sqls()?;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        println!("src: {}", TaskUtil::get_redis_version(&mut self.src_conn)?);
        println!("dst: {}", TaskUtil::get_redis_version(&mut self.dst_conn)?);

        self.execute_cmds(SRC, &self.base.src_dml_sqls.clone());
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_all_data()?;

        self.base.wait_task_finish(&task).await
    }

    pub fn execute_test_ddl_sqls(&mut self) -> Result<(), Error> {
        self.execute_cmds(SRC, &self.base.src_ddl_sqls.clone());
        self.execute_cmds(DST, &self.base.dst_ddl_sqls.clone());
        Ok(())
    }

    fn compare_all_data(&mut self) -> Result<(), Error> {
        let dbs = self.list_dbs(SRC);
        for db in dbs.iter() {
            println!("compare data for db: {}", db);
            self.compare_data(db)?;
        }
        Ok(())
    }

    fn compare_data(&mut self, db: &str) -> Result<(), Error> {
        self.execute_cmd(SRC, &format!("SELECT {}", db));
        self.execute_cmd(DST, &format!("SELECT {}", db));

        let mut string_keys = Vec::new();
        let mut hash_keys = Vec::new();
        let mut list_keys = Vec::new();
        let mut stream_keys = Vec::new();
        let mut set_keys = Vec::new();
        let mut zset_keys = Vec::new();

        let mut json_keys = Vec::new();
        let mut bf_bloom_keys = Vec::new();
        let mut cf_bloom_keys = Vec::new();

        let keys = self.list_keys(SRC, "*");
        for i in keys.iter() {
            let key = i.clone();
            let key_type = self.get_key_type(SRC, &key);
            match key_type.to_lowercase().as_str() {
                "string" => string_keys.push(key),
                "hash" => hash_keys.push(key),
                "zset" => zset_keys.push(key),
                "stream" => stream_keys.push(key),
                "set" => set_keys.push(key),
                "list" => list_keys.push(key),
                "rejson-rl" => json_keys.push(key),
                "mbbloom--" => bf_bloom_keys.push(key),
                "mbbloomcf" => cf_bloom_keys.push(key),
                _ => {
                    println!("unkown type: {} for key: {}", key_type, key);
                    string_keys.push(key)
                }
            }
        }

        self.compare_string_entries(db, &string_keys);
        self.compare_hash_entries(db, &hash_keys);
        self.compare_list_entries(db, &list_keys);
        self.compare_set_entries(db, &set_keys);
        self.compare_zset_entries(db, &zset_keys);
        self.compare_stream_entries(db, &stream_keys);
        self.compare_rejson_entries(db, &json_keys);
        self.compare_bf_bloom_entries(db, &bf_bloom_keys);
        self.compare_cf_bloom_entries(db, &cf_bloom_keys);
        self.check_expire(&keys);
        Ok(())
    }

    fn check_expire(&mut self, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("PTTL {}", self.escape_key(key));
            let src_result = self.execute_cmd(SRC, &cmd);
            let dst_result = self.execute_cmd(DST, &cmd);

            let get_expire = |result: Value| -> i64 {
                match result {
                    Value::Int(expire) => return expire,
                    _ => {
                        // should never happen
                        return -1000;
                    }
                }
            };

            let src_expire = get_expire(src_result);
            let dst_expire = get_expire(dst_result);
            assert_ne!(src_expire, -1000);
            assert_ne!(dst_expire, -1000);
            if src_expire > 0 {
                println!("src key: {} expire in {}", key, src_expire);
                println!("dst key: {} expire in {}", key, dst_expire);
                assert!(dst_expire > 0)
            } else {
                assert!(dst_expire < 0)
            }
        }
    }

    fn compare_string_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("GET {}", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_hash_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("HGETALL {}", self.escape_key(key));
            let src_result = self.execute_cmd(SRC, &cmd);
            let dst_result = self.execute_cmd(DST, &cmd);

            let build_kvs = |result: redis::Value| {
                let mut kvs = HashMap::new();
                if let redis::Value::Bulk(mut values) = result {
                    for _i in (0..values.len()).step_by(2) {
                        let k = values.remove(0);
                        let v = values.remove(0);
                        if let redis::Value::Data(k) = k {
                            kvs.insert(k, v);
                        } else {
                            assert!(false);
                        }
                    }
                } else {
                    assert!(false);
                }
                kvs
            };

            let src_kvs = build_kvs(src_result);
            let dst_kvs = build_kvs(dst_result);
            println!(
                "compare results for cmd: {}, \r\n src_kvs: {:?} \r\n dst_kvs: {:?}",
                cmd, src_kvs, dst_kvs
            );

            if self.filter.filter_db(db) {
                println!("filtered, db: {}, key: {}", db, key);
                assert_eq!(dst_kvs.len(), 0);
            } else {
                assert_eq!(src_kvs, dst_kvs);
            }
        }
    }

    fn compare_list_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("LRANGE {} 0 -1", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_set_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("SORT {} ALPHA", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_zset_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("ZRANGE {} 0 -1 WITHSCORES", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_stream_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("XRANGE {} - +", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_rejson_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("JSON.GET {}", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_bf_bloom_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("BF.DEBUG {}", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_cf_bloom_entries(&mut self, db: &str, keys: &Vec<String>) {
        for key in keys {
            let cmd = format!("CF.DEBUG {}", self.escape_key(key));
            self.compare_cmd_results(&cmd, db, key);
        }
    }

    fn compare_cmd_results(&mut self, cmd: &str, db: &str, key: &str) {
        let src_result = self.execute_cmd(SRC, cmd);
        let dst_result = self.execute_cmd(DST, cmd);
        println!(
            "compare results for cmd: {}, \r\n src_kvs: {:?} \r\n dst_kvs: {:?}",
            cmd, src_result, dst_result
        );

        if self.filter.filter_db(db) {
            println!("filtered, db: {}, key: {}", db, key);
            match dst_result {
                Value::Bulk(v) => assert_eq!(v, vec![]),
                _ => assert_eq!(dst_result, Value::Nil),
            }
        } else {
            assert_eq!(src_result, dst_result);
        }
    }

    fn list_dbs(&mut self, from: &str) -> Vec<String> {
        let mut dbs = Vec::new();
        let cmd = "INFO keyspace";
        match self.execute_cmd(from, &cmd) {
            redis::Value::Data(data) => {
                let spaces = String::from_utf8(data).unwrap();
                for space in spaces.split("\r\n").collect::<Vec<&str>>() {
                    if space.contains("db") {
                        let tokens: Vec<&str> = space.split(":").collect::<Vec<&str>>();
                        dbs.push(tokens[0].trim_start_matches("db").to_string());
                    }
                }
            }
            _ => {}
        }
        dbs
    }

    fn list_keys(&mut self, from: &str, match_pattern: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let cmd = format!("KEYS {}", match_pattern);
        match self.execute_cmd(from, &cmd) {
            redis::Value::Bulk(values) => {
                for v in values {
                    match v {
                        redis::Value::Data(data) => {
                            let key = String::from_utf8(data).unwrap();
                            if SYSTEM_KEYS.contains(&key.as_str()) {
                                continue;
                            }
                            keys.push(key)
                        }
                        _ => assert!(false),
                    }
                }
            }
            _ => assert!(false),
        }
        keys.sort();
        keys
    }

    fn get_key_type(&mut self, from: &str, key: &str) -> String {
        let cmd = format!("type {}", self.escape_key(key));
        let value = self.execute_cmd(from, &cmd);
        match value {
            redis::Value::Status(key_type) => {
                return key_type;
            }
            _ => assert!(false),
        }
        String::new()
    }

    fn escape_key(&self, key: &str) -> String {
        format!(
            "{}{}{}",
            self.escape_pairs[0].0, key, self.escape_pairs[0].1
        )
    }

    fn execute_cmds(&mut self, from: &str, cmds: &Vec<String>) {
        for cmd in cmds.iter() {
            self.execute_cmd(from, cmd);
        }
    }

    fn execute_cmd(&mut self, from: &str, cmd: &str) -> Value {
        println!("execute cmd: {:?}", cmd);
        let packed_cmd = self.pack_cmd(cmd);
        let conn = if from == SRC {
            &mut self.src_conn
        } else {
            &mut self.dst_conn
        };
        conn.req_packed_command(&packed_cmd).unwrap()
    }

    fn pack_cmd(&self, cmd: &str) -> Vec<u8> {
        // parse cmd args
        let mut redis_cmd = RedisCmd::new();
        for arg in ConfigTokenParser::parse(cmd, &self.delimiters, &self.escape_pairs) {
            let mut arg = arg.clone();
            for (left, right) in &self.escape_pairs {
                arg = arg
                    .trim_start_matches(*left)
                    .trim_end_matches(*right)
                    .to_string();
            }
            redis_cmd.add_str_arg(&arg);
        }
        CmdEncoder::encode(&redis_cmd)
    }
}
