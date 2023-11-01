#[cfg(test)]
mod test {
    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/basic_test", 2000, 10000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_multi_dbs_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/multi_dbs_test", 2000, 10000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_multi_exec_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/multi_exec_test", 2000, 10000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_cmds_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/cmds_test", 2000, 15000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_filter_db_test() {
        TestBase::run_redis_cdc_test("redis_to_redis/cdc/7_0/filter_db_test", 2000, 10000).await;
    }
}
