#[cfg(test)]
mod test {

    use crate::test_runner::test_base::TestBase;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_multi_dbs_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/multi_dbs_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_cmds_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/cmds_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_hash_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/hash_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_list_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/list_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_set_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/set_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_stream_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/stream_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_string_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/string_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_zset_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/zset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_length_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/length_test").await;
    }

    // test sinking rdb data by rewrite instead of restore
    #[tokio::test]
    #[serial]
    async fn cdc_rewrite_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/rewrite_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_rewrite_stream_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/rewrite_stream_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_filter_db_test() {
        TestBase::run_redis_snapshot_test("redis_to_redis/snapshot/7_0/filter_db_test").await;
    }
}
