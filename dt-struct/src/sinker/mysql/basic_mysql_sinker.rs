use async_trait::async_trait;
use dt_common::config::{router_config::RouterConfig, sinker_config::SinkerConfig};
use sqlx::{mysql::MySqlPoolOptions, query, MySql, Pool};

use crate::{
    error::Error,
    meta::common::database_model::{Column, IndexKind, StructModel},
    traits::StructSinker,
};

pub struct MySqlStructSinker {
    pub pool: Option<Pool<MySql>>,
    pub sinker_config: SinkerConfig,
    pub router_config: RouterConfig,
}

#[async_trait]
impl StructSinker for MySqlStructSinker {
    // fn support_db_type() {}
    // fn is_db_version_supported(_db_version: String) {}

    async fn build_connection(&mut self) -> Result<(), Error> {
        match &self.sinker_config {
            SinkerConfig::BasicConfig { url, db_type: _ } => {
                let db_pool = MySqlPoolOptions::new().connect(&url).await?;
                self.pool = Option::Some(db_pool);
            }
            _ => {}
        };
        Ok(())
    }

    async fn sink_from_queue(&self, model: &mut StructModel) -> Result<(), Error> {
        let mysql_pool: &Pool<MySql>;
        match &self.pool {
            Some(p) => mysql_pool = &p,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        match model {
            StructModel::TableModel {
                database_name,
                schema_name: _,
                table_name,
                engine_name,
                table_comment,
                columns,
            } => {
                let (column_str, pk_arr, global_charset, global_collation) =
                    build_sql_with_table_columns(columns).unwrap();
                let mut pk_str = String::from("");
                if pk_arr.len() > 0 {
                    pk_str = format!(
                        ",PRIMARY KEY ({})",
                        pk_arr
                            .iter()
                            .map(|x| format!("`{}`", x))
                            .collect::<Vec<String>>()
                            .join(",")
                    )
                }
                // Todo: table partition; column visible, generated
                let mut sql = format!(
                    "CREATE TABLE `{}`.`{}` ({}{}) ENGINE={} ",
                    database_name, table_name, column_str, pk_str, engine_name
                );
                if !global_charset.is_empty() {
                    sql.push_str(format!("DEFAULT CHARSET={} ", global_charset).as_str())
                }
                if !global_collation.is_empty() {
                    sql.push_str(format!("COLLATE={} ", global_collation).as_str())
                }
                if !table_comment.is_empty() {
                    sql.push_str(format!("COMMENT='{}' ", table_comment).as_str());
                }
                match query(&sql).execute(mysql_pool).await {
                    Ok(_) => {
                        return {
                            println!("create table sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "create table sql:[{}],execute failed:{}",
                                sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::IndexModel {
                database_name,
                schema_name: _,
                table_name,
                index_name,
                index_kind,
                index_type,
                comment,
                tablespace: _,
                definition: _,
                columns,
            } => {
                // Todo: fk?
                //     CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX index_name
                //         [index_type]
                //         ON tbl_name (key_part,...)
                //         [index_option]
                //         [algorithm_option | lock_option] ...
                //
                //     key_part:
                //         col_name [(length)] [ASC | DESC]
                //
                //     index_option:
                //         KEY_BLOCK_SIZE [=] value (Todo:)
                //     | index_type
                //     | WITH PARSER parser_name (Todo:)
                //     | COMMENT 'string'
                //
                //     index_type:
                //         USING {BTREE | HASH}
                //
                //     algorithm_option(Todo:):
                //         ALGORITHM [=] {DEFAULT | INPLACE | COPY}
                //
                //     lock_option(Todo:):
                //         LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
                let mut index_kind_str = String::from("");
                match index_kind {
                    IndexKind::Unique => {
                        index_kind_str = String::from("UNIQUE");
                    }
                    _ => {}
                }
                columns.sort_by(|a, b| a.seq_in_index.cmp(&b.seq_in_index));
                let mut sql = format!(
                    "CREATE {} INDEX `{}` USING {} ON `{}`.`{}` ({}) ",
                    index_kind_str,
                    index_name,
                    index_type,
                    database_name,
                    table_name,
                    columns
                        .iter()
                        .filter(|x| !x.column_name.is_empty())
                        .map(|x| format!("`{}`", x.column_name))
                        .collect::<Vec<String>>()
                        .join(",")
                );
                if !comment.is_empty() {
                    sql.push_str(format!("COMMENT '{}' ", comment).as_str());
                }
                match query(&sql).execute(mysql_pool).await {
                    Ok(_) => {
                        return {
                            println!("create index sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "create index sql:[{}],execute failed:{}",
                                sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/**
 *  Return:
 *      all columns build sql
 *      primary key build sql
 *      global charset
 *      global collation
 */
fn build_sql_with_table_columns(
    cols: &mut Vec<Column>,
) -> Result<(String, Vec<String>, String, String), Error> {
    // order
    // default value
    // auto increment
    // `col1` {col_type} {nullable} {default_value} {auto_increment} comment {comment} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
    let (mut result_str, mut pk_str, mut global_charset, mut global_collation) = (
        String::from(""),
        Vec::new(),
        String::from(""),
        String::from(""),
    );
    cols.sort_by(|c1, c2| c1.order_position.cmp(&c2.order_position));

    let mut charsets: Vec<String> = cols
        .iter()
        .filter(|x| !x.character_set.is_empty())
        .map(|x| x.character_set.clone())
        .collect();
    MySqlStructSinker::sort_and_dedup(&mut charsets);
    let mut collations: Vec<String> = cols
        .iter()
        .filter(|x| !x.collation.is_empty())
        .map(|x| x.collation.clone())
        .collect();
    MySqlStructSinker::sort_and_dedup(&mut collations);
    if charsets.len() == 1 {
        global_charset = String::from(charsets.get(0).unwrap());
    }
    if collations.len() == 1 {
        global_collation = String::from(collations.get(0).unwrap());
    }

    for col in cols {
        let nullable: String;
        if col.is_nullable.to_lowercase() == "no" {
            nullable = String::from("NOT NULL");
        } else {
            nullable = String::from("NULL");
        }
        result_str
            .push_str(format!(" `{}` {} {} ", col.column_name, col.column_type, nullable).as_str());
        match &col.default_value {
            Some(v) => {
                if v.to_lowercase().starts_with("current_") {
                    result_str.push_str(format!("DEFAULT {} ", v).as_str());
                } else {
                    result_str.push_str(format!("DEFAULT '{}' ", v).as_str());
                }
            }
            None => {}
        }
        if !col.extra.is_empty() {
            // DEFAULT_GENERATED
            // DEFAULT_GENERATED on update CURRENT_TIMESTAMP
            result_str
                .push_str(format!("{} ", col.extra.replace("DEFAULT_GENERATED", "")).as_str());
        }
        if !col.column_comment.is_empty() {
            result_str.push_str(format!("COMMENT '{}' ", col.column_comment).as_str())
        }
        if global_charset.is_empty() && !col.character_set.is_empty() {
            result_str.push_str(format!("CHARACTER SET {} ", col.character_set).as_str())
        }
        if global_collation.is_empty() && !col.collation.is_empty() {
            result_str.push_str(format!("COLLATE {} ", col.collation).as_str())
        }
        result_str.push_str(",");
        if col.column_key == "PRI" {
            pk_str.push(String::from(col.column_name.as_str()));
        }
    }
    if result_str.ends_with(",") {
        result_str = result_str[0..result_str.len() - 1].to_string();
    }
    Ok((result_str, pk_str, global_charset, global_collation))
}

impl MySqlStructSinker {
    fn sort_and_dedup(arr: &mut Vec<String>) {
        arr.sort_by(|a, b| a.cmp(b));
        arr.dedup();
    }
}
