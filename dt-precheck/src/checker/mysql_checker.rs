use std::collections::HashSet;

use async_trait::async_trait;
use dt_common::config::{config_enums::DbType, filter_config::FilterConfig};
use dt_meta::struct_meta::db_table_model::DbTable;
use regex::Regex;

use crate::{
    config::precheck_config::PrecheckConfig,
    error::Error,
    fetcher::{mysql::mysql_fetcher::MysqlFetcher, traits::Fetcher},
    meta::{check_item::CheckItem, check_result::CheckResult},
};

use super::traits::Checker;

const MYSQL_SUPPORT_DB_VERSION_REGEX: &str = r"8\..*";

pub struct MySqlChecker {
    pub fetcher: MysqlFetcher,
    pub filter_config: FilterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
    pub db_type_option: Option<DbType>,
}

#[async_trait]
impl Checker for MySqlChecker {
    async fn build_connection(&mut self) -> Result<CheckResult, Error> {
        let mut check_error = None;
        let result = self.fetcher.build_connection().await;
        match result {
            Ok(_) => {}
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    // support MySQL 8.*
    async fn check_database_version(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

        let result = self.fetcher.fetch_version().await;
        match result {
            Ok(version) => {
                if version.is_empty() {
                    check_error = Some(Error::PreCheckError {
                        error: format!("found no version info."),
                    });
                } else {
                    let re = Regex::new(MYSQL_SUPPORT_DB_VERSION_REGEX).unwrap();
                    if !re.is_match(version.as_str()) {
                        check_error = Some(Error::PreCheckError {
                            error: format!("mysql version:[{}] is invalid.", version),
                        });
                    }
                }
            }
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_permission(&mut self) -> Result<CheckResult, Error> {
        Ok(CheckResult::build(
            CheckItem::CheckAccountPermission,
            self.is_source,
        ))
    }

    async fn check_cdc_supported(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

        if !self.is_source {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfDatabaseSupportCdc,
                self.is_source,
                self.db_type_option.clone(),
                check_error,
            ));
        }

        let mut errs: Vec<String> = vec![];
        let cdc_configs = vec!["log_bin", "binlog_format", "binlog_row_image"]
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<String>>();
        let result = self.fetcher.fetch_configuration(cdc_configs).await;
        match result {
            Ok(configs) => {
                for (k, v) in configs {
                    match k.as_str() {
                        "log_bin" => {
                            if v.to_lowercase() != "on" {
                                errs.push(format!(
                                    "log_bin setting:[{}] is not 'on'.",
                                    v.to_lowercase()
                                ));
                            }
                        }
                        "binlog_row_image" => {
                            if v.to_lowercase() != "full" {
                                errs.push(format!(
                                    "binlog_row_image setting:[{}] is not 'full'",
                                    v.to_lowercase()
                                ));
                            }
                        }
                        "binlog_format" => {
                            if v.to_lowercase() != "row" {
                                errs.push(format!(
                                    "binlog_format setting:[{}] is not 'row'.",
                                    v.to_lowercase()
                                ));
                            }
                        }
                        _ => {
                            return Err(Error::PreCheckError {
                                error: "find database cdc settings meet unknown error".to_string(),
                            })
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }
        if errs.len() > 0 {
            check_error = Some(Error::PreCheckError {
                error: errs.join(";"),
            })
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

        let (mut models, mut err_msgs): (Vec<DbTable>, Vec<String>) = (Vec::new(), Vec::new());
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut models)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut models)
                }
            }
        }
        let (dbs, tb_dbs, tbs) = DbTable::get_config_maps(&models).unwrap();
        let mut all_db_names = Vec::new();
        all_db_names.extend(&dbs);
        all_db_names.extend(&tb_dbs);

        if (self.is_source || !self.precheck_config.do_struct_init) && tbs.len() > 0 {
            // When a specific table to be migrated is specified and the following conditions are met, check the existence of the table
            // 1. this check is for the source database
            // 2. this check is for the sink database, and specified no structure initialization
            let current_tbs: HashSet<String>;
            let mut not_existed_tbs: HashSet<String> = HashSet::new();

            let tables_result = self.fetcher.fetch_tables().await;
            match tables_result {
                Ok(tables) => {
                    current_tbs = tables
                        .iter()
                        .map(|t| format!("{}.{}", t.database_name, t.table_name))
                        .collect()
                }
                Err(e) => return Err(e),
            }
            for tb in tbs {
                if !current_tbs.contains(&tb) {
                    not_existed_tbs.insert(tb);
                }
            }
            if not_existed_tbs.len() > 0 {
                err_msgs.push(format!(
                    "tables not existed: [{}]",
                    not_existed_tbs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(";")
                ));
            }
        }

        if all_db_names.len() > 0 {
            let current_dbs: HashSet<String>;
            let mut not_existed_dbs: HashSet<String> = HashSet::new();

            let dbs_result = self.fetcher.fetch_databases().await;
            match dbs_result {
                Ok(dbs) => {
                    current_dbs = dbs.iter().map(|d| d.database_name.clone()).collect();
                }
                Err(e) => return Err(e),
            }
            for db_name in all_db_names {
                if !current_dbs.contains(db_name) {
                    not_existed_dbs.insert(db_name.clone());
                }
            }
            if not_existed_dbs.len() > 0 {
                err_msgs.push(format!(
                    "databases not existed: [{}]",
                    not_existed_dbs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(";")
                ));
            }
        }
        if err_msgs.len() > 0 {
            check_error = Some(Error::PreCheckError {
                error: err_msgs.join("."),
            })
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_table_structs(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

        if !self.is_source {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                self.db_type_option.clone(),
                check_error,
            ));
        }

        let mut models: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut models)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut models)
                }
            }
        }
        let (dbs, tb_dbs, _) = DbTable::get_config_maps(&models).unwrap();
        let mut all_db_names = Vec::new();
        all_db_names.extend(&dbs);
        all_db_names.extend(&tb_dbs);

        let (mut has_pk_tables, mut has_fk_tables, mut no_pk_tables, mut err_msgs): (
            HashSet<String>,
            HashSet<String>,
            HashSet<String>,
            Vec<String>,
        ) = (HashSet::new(), HashSet::new(), HashSet::new(), Vec::new());

        let constraints_result = self.fetcher.fetch_constraints().await;
        match constraints_result {
            Ok(constraints) => {
                for constraint in constraints {
                    let db_tb_name =
                        format!("{}.{}", constraint.database_name, constraint.table_name);
                    match constraint.constraint_type.as_str() {
                        "PRIMARY KEY" => has_pk_tables.insert(db_tb_name),
                        "FOREIGN KEY" => has_fk_tables.insert(db_tb_name),
                        _ => true,
                    };
                }
            }
            Err(e) => return Err(e),
        }

        let tables_result = self.fetcher.fetch_tables().await;
        match tables_result {
            Ok(tables) => {
                for table in tables {
                    let db_tb_name = format!("{}.{}", table.database_name, table.table_name);
                    if !has_pk_tables.contains(&db_tb_name) {
                        no_pk_tables.insert(db_tb_name);
                    }
                }
            }
            Err(e) => return Err(e),
        }

        if has_fk_tables.len() > 0 {
            err_msgs.push(format!(
                "foreign keys are not supported, but these tables have foreign keys:[{}]",
                has_fk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ))
        }
        if no_pk_tables.len() > 0 {
            err_msgs.push(format!(
                "primary key are needed, but these tables don't have a primary key:[{}]",
                no_pk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ))
        }
        if err_msgs.len() > 0 {
            check_error = Some(Error::PreCheckError {
                error: err_msgs.join(";"),
            })
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }
}
