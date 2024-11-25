# Resume at breakpoint

Task progress will be recorded periodically in position.log / finished.log.

If a task interrupts, you need to restart it manually. By default, it will start from the beginning.

To avoid handling duplicate data, the task can resume at the breakpoint in position.log / finished.log.


## Supported
- MySQL as source
- Postgres as source
- Mongo as source

# Position Info
[position Info](../monitor/position.md)

## position.log
```
2024-10-10 04:04:08.152044 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db","tb":"b","order_col":"id","value":"6"}
2024-10-10 04:04:08.152181 | checkpoint_position | {"type":"None"}
```

## finished.log
```
2024-10-10 04:04:07.803422 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"a"}
2024-10-10 04:04:08.844988 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"b"}
```

# Configurations
## Resume from position.log & finished.log
```
[resumer]
resume_from_log=true
resume_log_dir=
```
- resume_log_dir is optional, defaults to the log directory of current task.
- tables in finished.log won't be migrated.
- uncompleted tables will be migrated from the breakpoint based on position.log.
- if a table does not have a single column **primary key/unique key**, no progress info will be in position.log, but it will be in finished.log once finished.

## Set resume config file
- you can choose another position info file besides resume_from_log.

```
[resumer]
resume_config_file=./resume.config
```

- resume.config has same contents as position.log/finished.log, example:

```
| current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db","tb":"a","order_col":"id","value":"6"}
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"d"}
```

- if a table exists in both position.log and resume.config, position.log will be used.

# Example
- task_config.ini
```
[resumer]
resume_from_log=true
resume_log_dir=./resume_logs
resume_config_file=./resume.config
```

- ./resume.config
```
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$1"}
{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$2"}
{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_col":"f_0","value":"5"}
{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_multi_uk","order_col":"f_0","value":"5"}
{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"resume_table_*$4","order_col":"p.k","value":"1"}
```

- ./resume_logs/finished.log
```
2024-04-01 07:08:05.459594 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"in_finished_log_table_*$1"}
2024-04-01 07:08:06.537135 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"in_finished_log_table_*$2"}
```

- ./resume_logs/position.log
```
2024-03-29 07:02:24.463776 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"in_position_log_table_*$1","order_col":"p.k","value":"0"}
2024-03-29 07:02:24.463777 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"in_position_log_table_*$1","order_col":"p.k","value":"1"}
```

- `test_db_@`.`finished_table_*$1`, `test_db_@`.`finished_table_*$2` are marked finished in resume.config.
- `test_db_@`.`in_finished_log_table_*$1`, `test_db_@`.`in_finished_log_table_*$2` are marked finished in finished.log.
- `test_db_1`.`one_pk_no_uk`, `test_db_1`.`one_pk_multi_uk`, `test_db_@`.`resume_table_*$4` have position info in resume.config.
- `test_db_@`.`in_position_log_table_*$1` have position info in position.log.


After task restarts, default.log:

```
2024-10-18 06:51:10.161794 - INFO - [1180981] - resumer, get resume value, schema: test_db_1, tb: one_pk_multi_uk, col: f_0, result: Some("5")
2024-10-18 06:51:11.193382 - INFO - [1180981] - resumer, get resume value, schema: test_db_1, tb: one_pk_no_uk, col: f_0, result: Some("5")
2024-10-18 06:51:12.135065 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: finished_table_*$1, result: true
2024-10-18 06:51:12.135186 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: finished_table_*$2, result: true
2024-10-18 06:51:12.135227 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: in_finished_log_table_*$1, result: true
2024-10-18 06:51:12.135265 - INFO - [1180981] - resumer, check finished: schema: test_db_@, tb: in_finished_log_table_*$2, result: true
2024-10-18 06:51:12.268390 - INFO - [1180981] - resumer, get resume value, schema: test_db_@, tb: in_position_log_table_*$1, col: p.k, result: Some("1")
2024-10-18 06:51:13.390645 - INFO - [1180981] - resumer, get resume value, schema: test_db_@, tb: resume_table_*$4, col: p.k, result: Some("1")
```

## Refer to tests
- dt-tests/tests/mysql_to_mysql/snapshot/resume_test
- dt-tests/tests/pg_to_pg/snapshot/resume_test
- dt-tests/tests/mongo_to_mongo/snapshot/resume_test