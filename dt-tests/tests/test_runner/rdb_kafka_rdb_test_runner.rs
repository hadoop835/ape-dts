use super::base_test_runner::BaseTestRunner;
use super::rdb_test_runner::RdbTestRunner;
use dt_common::config::sinker_config::SinkerConfig;
use dt_common::config::task_config::TaskConfig;
use dt_common::error::Error;
use dt_common::utils::time_util::TimeUtil;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use regex::Regex;

/// This is used for test cases: rdb(src) -> kafka -> rdb(dst).
/// There are 2 tasks running:
///     rdb(src) -> kafka
///     kafka -> rdb(dst)
/// And we need another dummy task runner to compare rdb(src) and rdb(dst)
///     rdb(src) -> rdb(dst)
pub struct RdbKafkaRdbTestRunner {
    src_to_dst_runner: RdbTestRunner,
    src_to_kafka_runner: BaseTestRunner,
    kafka_to_dst_runner: BaseTestRunner,
}

#[allow(dead_code)]
impl RdbKafkaRdbTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let src_to_dst_runner =
            RdbTestRunner::new(&format!("{}/src_to_dst", relative_test_dir)).await?;
        let src_to_kafka_runner =
            BaseTestRunner::new(&format!("{}/src_to_kafka", relative_test_dir)).await?;
        let kafka_to_dst_runner =
            BaseTestRunner::new(&format!("{}/kafka_to_dst", relative_test_dir)).await?;
        Ok(Self {
            src_to_dst_runner,
            src_to_kafka_runner,
            kafka_to_dst_runner,
        })
    }

    pub async fn run_snapshot_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<(), Error> {
        self.src_to_dst_runner.execute_test_ddl_sqls().await?;
        self.prepare_kafka().await?;
        // wait for topic creation
        TimeUtil::sleep_millis(start_millis).await;

        // prepare src data
        self.src_to_dst_runner.execute_test_dml_sqls().await?;

        // kafka -> dst
        let kafka_to_dst_task = self.kafka_to_dst_runner.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // src -> kafka
        self.src_to_kafka_runner.start_task().await?;
        TimeUtil::sleep_millis(parse_millis).await;

        // compare data
        let db_tbs = self.src_to_dst_runner.get_compare_db_tbs().await?;
        assert!(
            self.src_to_dst_runner
                .compare_data_for_tbs(&db_tbs.clone(), &db_tbs.clone())
                .await?
        );

        // stop
        self.kafka_to_dst_runner
            .wait_task_finish(&kafka_to_dst_task)
            .await?;

        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> Result<(), Error> {
        self.src_to_dst_runner.execute_test_ddl_sqls().await?;
        self.prepare_kafka().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // kafka -> dst & src -> kafka
        let kafka_to_dst_task = self.kafka_to_dst_runner.spawn_task().await?;
        let src_to_kafka_task = self.src_to_kafka_runner.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // execute test sqls and compare
        self.src_to_dst_runner
            .execute_test_sqls_and_compare(parse_millis)
            .await?;

        // stop
        self.kafka_to_dst_runner
            .wait_task_finish(&kafka_to_dst_task)
            .await?;
        self.src_to_kafka_runner
            .wait_task_finish(&src_to_kafka_task)
            .await?;
        Ok(())
    }

    async fn prepare_kafka(&self) -> Result<(), Error> {
        let mut topics: Vec<String> = vec![];
        for sql in self.src_to_kafka_runner.dst_ddl_sqls.iter() {
            let re = Regex::new(r"create topic ([\w\W]+)").unwrap();
            let cap = re.captures(sql).unwrap();
            topics.push(cap.get(1).unwrap().as_str().into());
        }

        let config = TaskConfig::new(&self.src_to_kafka_runner.task_config_file);
        match config.sinker {
            SinkerConfig::Kafka { url, .. } => {
                let client = Self::create_kafka_admin_client(&url);
                for topic in topics.iter() {
                    Self::delete_topic(&client, topic).await;
                    Self::create_topic(&client, topic).await;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn create_kafka_admin_client(url: &str) -> AdminClient<DefaultClientContext> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", url);
        config.set("session.timeout.ms", "10000");
        let client: AdminClient<DefaultClientContext> = config.create().unwrap();
        client
    }

    async fn create_topic(client: &AdminClient<DefaultClientContext>, topic: &str) {
        let topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        client
            .create_topics(&[topic], &AdminOptions::new())
            .await
            .unwrap();
    }

    async fn delete_topic(client: &AdminClient<DefaultClientContext>, topic: &str) {
        client
            .delete_topics(&[topic], &AdminOptions::new())
            .await
            .unwrap();
    }
}
