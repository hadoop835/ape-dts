use async_trait::async_trait;
use dt_common::{
    config::{router_config::RouterConfig, sinker_config::SinkerConfig},
    meta::postgresql::pg_enums::ConstraintTypeEnum,
};
use sqlx::{postgres::PgPoolOptions, query, Pool, Postgres};

use crate::{
    error::Error,
    meta::common::database_model::{Column, StructModel},
    traits::StructSinker,
};

pub struct PgStructSinker {
    pub pool: Option<Pool<Postgres>>,
    pub sinker_config: SinkerConfig,
    pub router_config: RouterConfig,
}

#[async_trait]
impl StructSinker for PgStructSinker {
    // fn support_db_type() {}
    // fn is_db_version_supported(_db_version: String) {}

    async fn build_connection(&mut self) -> Result<(), Error> {
        match &self.sinker_config {
            SinkerConfig::BasicConfig { url, db_type: _ } => {
                let db_pool = PgPoolOptions::new().connect(&url).await?;
                self.pool = Option::Some(db_pool);
            }
            _ => {}
        };
        Ok(())
    }

    async fn sink_from_queue(&self, model: &mut StructModel) -> Result<(), Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(p) => pg_pool = &p,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        match model {
            StructModel::TableModel {
                database_name: _,
                schema_name,
                table_name,
                engine_name: _,
                table_comment: _,
                columns,
            } => {
                let col_sql = PgStructSinker::build_columns_sql_str(columns).unwrap();
                let sql = format!(
                    "CREATE TABLE \"{}\".\"{}\" ({})",
                    schema_name, table_name, col_sql
                );
                match query(&sql).execute(pg_pool).await {
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
                database_name: _,
                schema_name: _,
                table_name: _,
                index_name: _,
                index_kind: _,
                index_type: _,
                comment: _,
                tablespace,
                definition,
                columns: _,
            } => {
                let sql = format!(
                    "{} TABLESPACE {}",
                    definition
                        .replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
                        .replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS"),
                    tablespace
                );
                match query(&sql).execute(pg_pool).await {
                    Ok(_) => {
                        return {
                            println!("add index sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!("add index sql:[{}],execute failed:{}", sql, e.to_string());
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::ConstraintModel {
                database_name: _,
                schema_name,
                table_name,
                constraint_name,
                constraint_type,
                definition,
            } => {
                if constraint_type == &ConstraintTypeEnum::Foregin.to_charval().unwrap() {
                    let msg = format!("foreign key is not supported yet, schema:[{}], table:[{}], constraint_name:[{}]", schema_name, table_name, constraint_name);
                    println!("{}", msg);
                    return Err(Error::StructError { error: msg });
                }
                let sql = format!(
                    "ALTER TABLE \"{}\".\"{}\" ADD CONSTRAINT \"{}\" {}",
                    schema_name, table_name, constraint_name, definition
                );
                match query(&sql).execute(pg_pool).await {
                    Ok(_) => {
                        return {
                            println!("add costraint sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "add costraint sql:[{}],execute failed:{}",
                                sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::SequenceModel {
                sequence_name,
                database_name: _,
                schema_name,
                data_type,
                start_value,
                increment,
                min_value,
                max_value,
                is_circle,
            } => {
                let mut cycle_str = String::from("NO CYCLE");
                if is_circle.to_lowercase() == "yes" {
                    cycle_str = String::from("CYCLE");
                }
                let create_sql = format!("CREATE SEQUENCE \"{}\".\"{}\" AS {} START {} INCREMENT by {} MINVALUE {} MAXVALUE {} {}", schema_name, sequence_name, data_type, start_value, increment, min_value, max_value, cycle_str);
                match query(&create_sql).execute(pg_pool).await {
                    Ok(_) => {
                        return {
                            println!(
                                "add sequence:[{}], sql:[{}],execute success",
                                sequence_name, create_sql
                            );
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "add sequence:[{}], sql:[{}],execute failed:{}",
                                sequence_name,
                                create_sql,
                                e.to_string()
                            );
                            Err(Error::from(e))
                        }
                    }
                }
            }
            StructModel::SequenceOwnerModel {
                sequence_name,
                database_name: _,
                schema_name,
                owner_table_name,
                owner_table_column_name,
            } => {
                if !owner_table_name.is_empty() && !owner_table_column_name.is_empty() {
                    let create_owner_sql = format!(
                        "ALTER SEQUENCE \"{}\".\"{}\" OWNED BY \"{}\".\"{}\".\"{}\"",
                        schema_name,
                        sequence_name,
                        schema_name,
                        owner_table_name,
                        owner_table_column_name
                    );
                    match query(&create_owner_sql).execute(pg_pool).await {
                        Ok(_) => {
                            return {
                                println!(
                                    "add ownership for sequence:[{}], sql:[{}], execute success",
                                    sequence_name, create_owner_sql
                                );
                                Ok(())
                            }
                        }
                        Err(e) => {
                            return {
                                println!(
                                    "add ownership for sequence:[{}], sql:[{}], execute success,execute failed:{}",
                                    sequence_name,
                                    create_owner_sql,
                                    e.to_string()
                                );
                                Err(Error::from(e))
                            }
                        }
                    }
                } else {
                    println!(
                        "schema:[{}.{}] has no ownership.",
                        schema_name, sequence_name
                    );
                }
            }
            StructModel::CommentModel {
                database_name: _,
                schema_name,
                table_name,
                column_name,
                comment,
            } => {
                if (table_name.is_empty() && column_name.is_empty()) || comment.is_empty() {
                    return Ok(());
                }
                let sql;
                if !column_name.is_empty() {
                    sql = format!(
                        "COMMENT ON COLUMN \"{}\".\"{}\".\"{}\" IS '{}'",
                        schema_name, table_name, column_name, comment
                    )
                } else {
                    sql = format!(
                        "COMMENT ON TABLE \"{}\".\"{}\" is '{}'",
                        schema_name, table_name, comment
                    )
                }
                match query(&sql).execute(pg_pool).await {
                    Ok(_) => {
                        return {
                            println!("create comment sql:[{}],execute success", sql);
                            Ok(())
                        }
                    }
                    Err(e) => {
                        return {
                            println!(
                                "create comment sql:[{}],execute failed:{}",
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

impl PgStructSinker {
    fn build_columns_sql_str(columns: &mut Vec<Column>) -> Option<String> {
        let mut result_str = String::from("");
        columns.sort_by(|a, b| a.order_position.cmp(&b.order_position));
        for column in columns {
            result_str
                .push_str(format!("\"{}\" {} ", column.column_name, column.column_type).as_str());
            if column.is_nullable.to_lowercase() == "no" {
                result_str.push_str("NOT NULL ");
            }
            match &column.default_value {
                Some(x) => result_str.push_str(format!("DEFAULT {} ", x).as_str()),
                None => {}
            }
            match &column.generated {
                Some(x) => {
                    if x == "ALWAYS" {
                        result_str.push_str("GENERATED ALWAYS AS IDENTITY ")
                    } else {
                        result_str.push_str("GENERATED BY DEFAULT AS IDENTITY ")
                    }
                }
                None => {}
            }
            result_str.push_str(",");
        }
        if result_str.ends_with(",") {
            result_str = result_str[0..result_str.len() - 1].to_string();
        }
        Some(result_str)
    }
}
