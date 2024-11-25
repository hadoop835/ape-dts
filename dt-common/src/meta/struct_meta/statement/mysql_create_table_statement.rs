use crate::{config::config_enums::DbType, rdb_filter::RdbFilter};

use crate::meta::struct_meta::structure::{
    column::Column,
    constraint::Constraint,
    index::{Index, IndexKind},
    structure_type::StructureType,
    table::Table,
};

#[derive(Debug, Clone)]
pub struct MysqlCreateTableStatement {
    pub table: Table,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
}

impl MysqlCreateTableStatement {
    pub fn route(&mut self, dst_db: &str, dst_tb: &str) {
        self.table.database_name = dst_db.to_string();
        self.table.table_name = dst_tb.to_string();

        for index in self.indexes.iter_mut() {
            index.database_name = dst_db.to_string();
            index.table_name = dst_tb.to_string();
        }

        for constraint in self.constraints.iter_mut() {
            constraint.database_name = dst_db.to_string();
            constraint.table_name = dst_tb.to_string();
        }
    }

    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();

        if !filter.filter_structure(&StructureType::Table) {
            let key = format!(
                "table.{}.{}",
                self.table.database_name, self.table.table_name
            );
            sqls.push((key, Self::table_to_sql(&mut self.table)));
        }

        for i in self.indexes.iter_mut() {
            match i.index_kind {
                IndexKind::Unique => {
                    if filter.filter_structure(&StructureType::Table) {
                        continue;
                    }
                }
                _ => {
                    if filter.filter_structure(&StructureType::Index) {
                        continue;
                    }
                }
            }

            let key = format!(
                "index.{}.{}.{}",
                i.database_name, i.table_name, i.index_name
            );
            sqls.push((key, Self::index_to_sql(i)));
        }

        if !filter.filter_structure(&StructureType::Constraint) {
            for i in self.constraints.iter() {
                let key = format!(
                    "constraint.{}.{}.{}",
                    i.database_name, i.table_name, i.constraint_name
                );
                sqls.push((key, Self::constraint_to_sql(i)));
            }
        }

        Ok(sqls)
    }

    fn table_to_sql(table: &mut Table) -> String {
        let (columns_sql, pks) = Self::columns_to_sql(&mut table.columns);
        let mut pk_str = String::new();
        if !pks.is_empty() {
            pk_str = format!(
                ", PRIMARY KEY ({})",
                pks.iter()
                    .map(|x| format!("`{}`", x))
                    .collect::<Vec<String>>()
                    .join(",")
            )
        }

        // Todo: table partition; column visible, generated(information_schema.column.GENERATION_EXPRESSION)
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}{})",
            table.database_name, table.table_name, columns_sql, pk_str
        );

        if !table.engine_name.is_empty() {
            sql = format!("{} ENGINE={} ", sql, table.engine_name);
        }

        if !table.character_set.is_empty() {
            sql = format!("{} DEFAULT CHARSET={}", sql, table.character_set);
        }

        if !table.table_collation.is_empty() {
            sql = format!("{} COLLATE={}", sql, table.table_collation);
        }

        if !table.table_comment.is_empty() {
            sql = format!("{} COMMENT='{}'", sql, table.table_comment);
        }

        sql
    }

    fn columns_to_sql(columns: &mut Vec<Column>) -> (String, Vec<String>) {
        let (mut sql, mut pks) = (String::new(), Vec::new());

        columns.sort_by(|c1, c2| c1.ordinal_position.cmp(&c2.ordinal_position));
        for i in columns {
            sql.push_str(&format!("`{}` {} ", i.column_name, i.column_type));

            if !i.character_set_name.is_empty() {
                sql.push_str(&format!("CHARACTER SET {} ", i.character_set_name))
            }

            if !i.collation_name.is_empty() {
                sql.push_str(&format!("COLLATE {} ", i.collation_name))
            }

            if let Some(v) = &i.column_default {
                if v.to_lowercase().starts_with("current_") {
                    sql.push_str(&format!("DEFAULT {} ", v));
                } else {
                    sql.push_str(&format!("DEFAULT '{}' ", v));
                }
            }

            if !i.extra.is_empty() {
                // DEFAULT_GENERATED
                // DEFAULT_GENERATED on update CURRENT_TIMESTAMP
                sql.push_str(&format!("{} ", i.extra.replace("DEFAULT_GENERATED", "")));
            }

            let nullable = if !i.is_nullable {
                String::from("NOT NULL ")
            } else {
                String::from("NULL ")
            };

            if !i.column_comment.is_empty() {
                sql.push_str(&format!("COMMENT '{}' ", i.column_comment))
            }

            sql.push_str(&format!("{} ", nullable));

            sql.push(',');

            if i.column_key == "PRI" {
                pks.push(i.column_name.clone());
            }
        }

        if sql.ends_with(',') {
            sql = sql[0..sql.len() - 1].to_string();
        }

        (sql, pks)
    }

    fn index_to_sql(index: &mut Index) -> String {
        index
            .columns
            .sort_by(|a, b| a.seq_in_index.cmp(&b.seq_in_index));
        let columns_sql = index
            .columns
            .iter()
            .filter(|x| !x.column_name.is_empty())
            .map(|x| format!("`{}`", x.column_name))
            .collect::<Vec<String>>()
            .join(",");

        let mut sql = format!(
            // no need index_type in "CREATE {} INDEX `{}` USING {IndexType}"
            // since only BETREE supported in both InnoDB and MyISAM
            // refer: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
            "CREATE {} INDEX `{}` ON `{}`.`{}` ({}) ",
            index.index_kind, index.index_name, index.database_name, index.table_name, columns_sql
        );

        if !index.comment.is_empty() {
            sql.push_str(&format!("COMMENT '{}' ", index.comment));
        }

        sql
    }

    fn constraint_to_sql(constraint: &Constraint) -> String {
        // TODO, check for escapes
        format!(
            "ALTER TABLE `{}`.`{}` ADD CONSTRAINT `{}` {} {} ",
            constraint.database_name,
            constraint.table_name,
            constraint.constraint_name,
            constraint.constraint_type.to_str(DbType::Mysql),
            constraint.definition
        )
    }
}