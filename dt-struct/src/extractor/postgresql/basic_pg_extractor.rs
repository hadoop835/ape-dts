use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{extractor_config::ExtractorConfig, filter_config::FilterConfig},
    meta::db_table_model::DbTable,
};
use futures::TryStreamExt;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    query, Pool, Postgres, Row,
};

use crate::{
    error::Error,
    meta::common::{
        col_model::ColType,
        database_model::{Column, IndexKind, StructModel},
    },
    traits::StructExtrator,
    utils::queue_operator::QueueOperator,
};

// Refer to: https://github.com/MichaelDBA/pg_get_tabledef/blob/main/pg_get_tabledef.sql
// Todo: character_set, partition
// not support column type: array[][]; user defined type
pub struct PgStructExtractor<'a> {
    pub pool: Option<Pool<Postgres>>,
    pub struct_obj_queue: &'a ConcurrentQueue<StructModel>,
    pub source_config: ExtractorConfig,
    pub filter_config: FilterConfig,
    pub is_finished: Arc<AtomicBool>,

    pub seq_owners: HashMap<String, StructModel>,
}

#[async_trait]
impl StructExtrator for PgStructExtractor<'_> {
    // fn support_db_type() {}
    // fn is_db_version_supported(_db_version: String) {}

    fn set_finished(&self) -> Result<(), Error> {
        self.is_finished.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_finished(&self) -> Result<bool, Error> {
        Ok(self.is_finished.load(Ordering::SeqCst))
    }

    async fn build_connection(&mut self) -> Result<(), Error> {
        match &self.source_config {
            ExtractorConfig::BasicConfig { url, db_type: _ } => {
                let db_pool = PgPoolOptions::new()
                    .max_connections(8)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect(url)
                    .await?;
                self.pool = Option::Some(db_pool);
            }
            _ => {}
        };
        Ok(())
    }

    async fn get_sequence(&mut self) -> Result<(), Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut db_tables: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut db_tables)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut db_tables)
                }
            }
        }
        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.len() <= 0 {
            println!("found no schema need to do migrate");
            return Ok(());
        }

        let mut table_used_seqs: HashSet<String> = HashSet::new();
        let mut models: HashMap<String, StructModel> = HashMap::new();

        let table_sql = format!("SELECT c.table_schema,c.table_name,c.column_name, c.data_type, c.column_default  
        FROM information_schema.columns c where table_schema in ({}) and column_default is not null and column_default like 'nextval(%'
        ", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query sequence used by table sql: {}", table_sql);
        let mut rows = query(&table_sql).fetch(pg_pool);
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "table_schema").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "table_name").unwrap(),
            );
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            // build with default_value, such as nextval('table_test_name_seq'::regclass), find seq name
            match PgStructExtractor::get_seq_name_by_default_value(
                PgStructExtractor::get_str_with_null(&row, "column_default").unwrap(),
            ) {
                Some(seq_name) => table_used_seqs.insert(seq_name),
                None => false,
            };
        }
        if table_used_seqs.len() > 0 {
            // query target seq
            let seq_sql = format!("select sequence_catalog,sequence_schema,sequence_name,data_type,start_value,minimum_value,maximum_value,increment,cycle_option 
            from information_schema.sequences where sequence_schema in ({})", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
            println!("pg query sequence sql: {}", seq_sql);
            let mut rows = query(&seq_sql).fetch(pg_pool);
            while let Some(row) = rows.try_next().await? {
                let (schema_name, seq_name): (String, String) = (
                    PgStructExtractor::get_str_with_null(&row, "sequence_schema").unwrap(),
                    PgStructExtractor::get_str_with_null(&row, "sequence_name").unwrap(),
                );
                if schema_name.is_empty() || seq_name.is_empty() {
                    continue;
                }
                if !table_used_seqs.contains(&seq_name) || !all_schemas.contains(&&schema_name) {
                    continue;
                }
                models.insert(
                    format!("{}.{}", schema_name, seq_name),
                    StructModel::SequenceModel {
                        sequence_name: seq_name,
                        database_name: PgStructExtractor::get_str_with_null(
                            &row,
                            "sequence_catalog",
                        )
                        .unwrap(),
                        schema_name,
                        data_type: PgStructExtractor::get_str_with_null(&row, "data_type").unwrap(),
                        start_value: row.get("start_value"),
                        increment: row.get("increment"),
                        min_value: row.get("minimum_value"),
                        max_value: row.get("maximum_value"),
                        is_circle: PgStructExtractor::get_str_with_null(&row, "cycle_option")
                            .unwrap(),
                    },
                );
            }
            // query seq ownership, and put the StructModel into memory, will push to queue after get_tables
            let owner_sql = format!("select seq.relname,tab.relname as table_name, attr.attname as column_name, ns.nspname 
            from pg_class as seq 
            join pg_namespace ns on (seq.relnamespace = ns.oid) 
            join pg_depend as dep on (seq.relfilenode = dep.objid) 
            join pg_class as tab on (dep.refobjid = tab.relfilenode) 
            join pg_attribute as attr on (attr.attnum = dep.refobjsubid and attr.attrelid = dep.refobjid) 
            where dep.deptype='a' and ns.nspname in ({}) ", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
            println!("pg query sequence owner sql: {}", owner_sql);
            let mut rows = query(&owner_sql).fetch(pg_pool);
            while let Some(row) = rows.try_next().await? {
                let (schema_name, seq_name): (String, String) = (
                    PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                    PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
                );
                if schema_name.is_empty() && seq_name.is_empty() {
                    continue;
                }
                let schema_seq_name = format!("{}.{}", schema_name, seq_name);
                if (!table_used_seqs.contains(&seq_name) || !all_schemas.contains(&&schema_name))
                    || !models.contains_key(&schema_seq_name)
                {
                    continue;
                }
                if !self.seq_owners.contains_key(&schema_seq_name) {
                    self.seq_owners.insert(
                        schema_seq_name,
                        StructModel::SequenceOwnerModel {
                            sequence_name: seq_name,
                            database_name: String::from(""),
                            schema_name: schema_name,
                            owner_table_name: PgStructExtractor::get_str_with_null(
                                &row,
                                "table_name",
                            )
                            .unwrap(),
                            owner_table_column_name: PgStructExtractor::get_str_with_null(
                                &row,
                                "column_name",
                            )
                            .unwrap(),
                        },
                    );
                }
            }
        } else {
            println!("find no table used sequence");
        }
        if models.len() > 0 {
            for (_, seq_model) in &models {
                let _ = QueueOperator::push_to_queue(&self.struct_obj_queue, seq_model.clone(), 1)
                    .await;
            }
        }
        println!("get sequence finished");
        Ok(())
    }

    async fn get_table(&self) -> Result<(), Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut db_tables: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut db_tables)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut db_tables)
                }
            }
        }
        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.len() <= 0 {
            println!("found no schema need to do migrate");
            return Ok(());
        }
        let sql = format!("SELECT c.table_schema,c.table_name,c.column_name, c.data_type, c.udt_name, c.character_maximum_length, c.is_nullable, c.column_default, c.numeric_precision, c.numeric_scale, c.is_identity, c.identity_generation,c.ordinal_position 
        FROM information_schema.columns c where table_schema in ({}) ORDER BY table_schema,table_name", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query table sql: {}", sql);
        let mut rows = query(&sql).fetch(pg_pool);
        let mut models: HashMap<String, StructModel> = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "table_schema").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "table_name").unwrap(),
            );
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            if models.contains_key(&schema_table_name) {
                match models.get_mut(&schema_table_name).unwrap() {
                    StructModel::TableModel {
                        database_name: _,
                        schema_name: _,
                        table_name: _,
                        engine_name: _,
                        table_comment: _,
                        columns,
                    } => {
                        let order: i32 = row.try_get("ordinal_position")?;
                        columns.push(Column {
                            column_name: PgStructExtractor::get_str_with_null(&row, "column_name")
                                .unwrap(),
                            order_position: order as u32,
                            default_value: row.get("column_default"),
                            is_nullable: PgStructExtractor::get_str_with_null(&row, "is_nullable")
                                .unwrap(),
                            column_type: PgStructExtractor::get_col_data_type(
                                PgStructExtractor::get_str_with_null(&row, "udt_name").unwrap(),
                                PgStructExtractor::get_str_with_null(&row, "data_type").unwrap(),
                                PgStructExtractor::get_str_with_null(&row, "table_schema").unwrap(),
                                row.get("character_maximum_length"),
                                row.get("numeric_precision"),
                                row.get("numeric_scale"),
                            )
                            .unwrap(),
                            generated: PgStructExtractor::get_col_generated_rule(
                                row.get("is_identity"),
                                row.get("identity_generation"),
                            ),
                            column_key: String::from(""),
                            extra: String::from(""),
                            column_comment: String::from(""),
                            character_set: String::from(""),
                            collation: String::from(""),
                        })
                    }
                    _ => {}
                }
            } else {
                let order: i32 = row.try_get("ordinal_position")?;
                models.insert(
                    schema_table_name,
                    StructModel::TableModel {
                        database_name: String::from(""),
                        schema_name: PgStructExtractor::get_str_with_null(&row, "table_schema")
                            .unwrap(),
                        table_name: PgStructExtractor::get_str_with_null(&row, "table_name")
                            .unwrap(),
                        engine_name: String::from(""),
                        table_comment: String::from(""),
                        columns: vec![Column {
                            column_name: PgStructExtractor::get_str_with_null(&row, "column_name")
                                .unwrap(),
                            order_position: order as u32,
                            default_value: row.get("column_default"),
                            is_nullable: PgStructExtractor::get_str_with_null(&row, "is_nullable")
                                .unwrap(),
                            column_type: PgStructExtractor::get_col_data_type(
                                PgStructExtractor::get_str_with_null(&row, "udt_name").unwrap(),
                                PgStructExtractor::get_str_with_null(&row, "data_type").unwrap(),
                                PgStructExtractor::get_str_with_null(&row, "table_schema").unwrap(),
                                row.get("character_maximum_length"),
                                row.get("numeric_precision"),
                                row.get("numeric_scale"),
                            )
                            .unwrap(),
                            generated: PgStructExtractor::get_col_generated_rule(
                                row.get("is_identity"),
                                row.get("identity_generation"),
                            ),
                            column_key: String::from(""),
                            extra: String::from(""),
                            column_comment: String::from(""),
                            character_set: String::from(""),
                            collation: String::from(""),
                        }],
                    },
                );
            }
        }
        if models.len() > 0 {
            for (_, model) in &models {
                let _ =
                    QueueOperator::push_to_queue(&self.struct_obj_queue, model.clone(), 1).await;
            }
            // push the sequence related to those tables
            for (_, seq_model) in &self.seq_owners {
                let _ = QueueOperator::push_to_queue(&self.struct_obj_queue, seq_model.clone(), 1)
                    .await;
            }
        }
        println!("get tables finished");
        Ok(())
    }

    async fn get_constraint(&self) -> Result<(), Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut db_tables: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut db_tables)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut db_tables)
                }
            }
        }
        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.len() <= 0 {
            println!("found no schema need to do migrate");
            return Ok(());
        }
        let sql = format!("SELECT nsp.nspname, rel.relname, con.conname as constraint_name, con.contype as constraint_type,pg_get_constraintdef(con.oid) as constraint_definition
        FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
        WHERE nsp.nspname in ({}) order by nsp.nspname,rel.relname", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query constraint sql: {}", sql);
        let mut result: Vec<StructModel> = Vec::new();
        let mut rows = query(&sql).fetch(pg_pool);
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
            );
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            result.push(StructModel::ConstraintModel {
                database_name: String::from(""),
                schema_name: PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                table_name: PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
                constraint_name: PgStructExtractor::get_str_with_null(&row, "constraint_name")
                    .unwrap(),
                constraint_type: PgStructExtractor::get_with_null(
                    &row,
                    "constraint_type",
                    ColType::Char,
                )
                .unwrap(),
                definition: PgStructExtractor::get_str_with_null(&row, "constraint_definition")
                    .unwrap(),
            });
        }
        if result.len() > 0 {
            for model in &result {
                let _ =
                    QueueOperator::push_to_queue(&self.struct_obj_queue, model.clone(), 1).await;
            }
        }
        println!("get constraint finished");
        Ok(())
    }

    async fn get_index(&self) -> Result<(), Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut db_tables: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut db_tables)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut db_tables)
                }
            }
        }
        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.len() <= 0 {
            println!("found no schema need to do migrate");
            return Ok(());
        }
        let sql = format!("SELECT schemaname,tablename,indexdef, COALESCE(tablespace, 'pg_default') as tablespace, indexname FROM pg_indexes WHERE schemaname in ({})", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query index sql: {}", sql);
        let mut result: Vec<StructModel> = Vec::new();
        let mut rows = query(&sql).fetch(pg_pool);
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "schemaname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "tablename").unwrap(),
            );
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            result.push(StructModel::IndexModel {
                database_name: String::from(""),
                schema_name: PgStructExtractor::get_str_with_null(&row, "schemaname").unwrap(),
                table_name: PgStructExtractor::get_str_with_null(&row, "tablename").unwrap(),
                index_name: PgStructExtractor::get_str_with_null(&row, "indexname").unwrap(),
                index_kind: IndexKind::Index,
                index_type: String::from(""),
                comment: String::from(""),
                tablespace: PgStructExtractor::get_str_with_null(&row, "tablespace").unwrap(),
                definition: PgStructExtractor::get_str_with_null(&row, "indexdef").unwrap(),
                columns: Vec::new(),
            })
        }
        if result.len() > 0 {
            for model in &result {
                let _ =
                    QueueOperator::push_to_queue(&self.struct_obj_queue, model.clone(), 1).await;
            }
        }
        println!("get indexs finished");
        Ok(())
    }

    async fn get_comment(&self) -> Result<(), Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut db_tables: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut db_tables)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut db_tables)
                }
            }
        }
        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.len() <= 0 {
            println!("found no schema need to do migrate");
            return Ok(());
        }
        let mut result: Vec<StructModel> = Vec::new();

        let sql = format!("SELECT n.nspname, c.relname, d.description FROM pg_class c LEFT JOIN pg_namespace n on n.oid = c.relnamespace
        LEFT JOIN pg_description d ON c.oid = d.objoid AND d.objsubid = 0
        where nspname in ({}) and d.description is not null", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query table comment sql: {}", sql);
        let mut rows = query(&sql).fetch(pg_pool);
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
            );
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            result.push(StructModel::CommentModel {
                database_name: String::from(""),
                schema_name: PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                table_name: PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
                column_name: String::from(""),
                comment: PgStructExtractor::get_str_with_null(&row, "description").unwrap(),
            })
        }
        let col_sql = format!("SELECT n.nspname,c.relname, col_description(a.attrelid, a.attnum)as comment,format_type(a.atttypid, a.atttypmod)as type,a.attname as name,a.attnotnull as notnull
        FROM pg_class c LEFT JOIN pg_attribute a on a.attrelid =c.oid
        LEFT JOIN pg_namespace n on n.oid = c.relnamespace
        where n.nspname in ({}) and a.attnum >0 and col_description(a.attrelid, a.attnum) is not null", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query column comment sql: {}", col_sql);
        let mut col_rows = query(&col_sql).fetch(pg_pool);
        while let Some(row) = col_rows.try_next().await? {
            let (schema_name, table_name): (String, String) = (
                PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
            );
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            result.push(StructModel::CommentModel {
                database_name: String::from(""),
                schema_name: PgStructExtractor::get_str_with_null(&row, "nspname").unwrap(),
                table_name: PgStructExtractor::get_str_with_null(&row, "relname").unwrap(),
                column_name: PgStructExtractor::get_str_with_null(&row, "name").unwrap(),
                comment: PgStructExtractor::get_str_with_null(&row, "comment").unwrap(),
            })
        }
        if result.len() > 0 {
            for model in &result {
                let _ =
                    QueueOperator::push_to_queue(&self.struct_obj_queue, model.clone(), 1).await;
            }
        }
        println!("get comment finished");
        Ok(())
    }
}

impl PgStructExtractor<'_> {
    fn get_str_with_null(row: &PgRow, col_name: &str) -> Result<String, Error> {
        PgStructExtractor::get_with_null(row, col_name, ColType::Text)
    }
    fn get_with_null(row: &PgRow, col_name: &str, col_type: ColType) -> Result<String, Error> {
        let mut str_val = String::from("");
        match col_type {
            ColType::Text => {
                let str_val_option: Option<String> = row.get(col_name);
                match str_val_option {
                    Some(s) => str_val = s,
                    None => {}
                }
            }
            ColType::Char => {
                let char_val: i8 = row.get(col_name);
                str_val = char_val.to_string();
            }
        }
        Ok(str_val)
    }

    fn get_col_data_type(
        udt_name: String,
        data_type: String,
        schema_name: String,
        char_max_length: Option<i32>,
        num_percision: Option<i32>,
        num_scale: Option<i32>,
    ) -> Option<String> {
        let mut result_type = String::from("");
        let type_vec = vec![
            "geometry",
            "box2d",
            "box2df",
            "box3d",
            "geography",
            "geometry_dump",
            "gidx",
            "spheroid",
            "valid_detail",
            "_text",
        ];
        if type_vec.contains(&udt_name.as_str()) {
            result_type.push_str(udt_name.as_str());
        } else if data_type == "USER-DEFINED" {
            result_type.push_str(format!("{}.{}", schema_name, udt_name).as_str());
        } else {
            result_type.push_str(data_type.as_str());
        }
        if char_max_length.is_some() {
            result_type.push_str(format!("({})", char_max_length.unwrap()).as_str());
        } else if num_percision.is_some()
            && num_percision.unwrap() > 0
            && num_scale.is_some()
            && num_scale.unwrap() > 0
        {
            result_type
                .push_str(format!("({},{})", num_percision.unwrap(), num_scale.unwrap()).as_str())
        }
        Some(result_type)
    }

    fn get_col_generated_rule(
        is_identity: Option<String>,
        identity_generation: Option<String>,
    ) -> Option<String> {
        match is_identity {
            Some(i) => {
                if i.to_lowercase() == "yes" && identity_generation.is_some() {
                    return identity_generation;
                }
            }
            None => {}
        }
        None
    }

    fn get_seq_name_by_default_value(default_value: String) -> Option<String> {
        // default_value such as:
        //   nextval('table_test_name_seq'::regclass)
        //   nextval('"table_test_name_seq"')
        //   nextval('struct_it.full_column_type_id_seq'::regclass)
        if default_value.is_empty() || !default_value.starts_with("nextval(") {
            return None;
        }
        let arr_tmp: Vec<&str> = default_value.split("'").collect();
        if arr_tmp.len() != 3 {
            println!(
                "default_value:[{}] is like a sequence used, but not valid in process.",
                default_value
            );
            return None;
        }
        let mut seq_name = arr_tmp[1];
        if seq_name.contains(".") {
            let real_name_start_index = seq_name.find(".").unwrap() + 1;
            seq_name = &seq_name[real_name_start_index..seq_name.len()];
        }
        if seq_name.starts_with("\"") && seq_name.ends_with("\"") {
            let (start_index, end_index) = (
                seq_name.find("\"").unwrap() + 1,
                seq_name.rfind("\"").unwrap(),
            );
            seq_name = &seq_name[start_index..end_index];
        }
        Some(seq_name.to_string())
    }
}
