use std::{collections::HashMap, str::FromStr};

use apache_avro::{from_avro_datum, to_avro_datum, types::Value, Schema};
use dt_common::error::Error;

use crate::{
    col_value::ColValue, rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType,
};

use super::avro_converter_schema::{AvroConverterSchema, AvroFieldDef};

#[derive(Clone)]
pub struct AvroConverter {
    schema: Schema,
    pub meta_manager: Option<RdbMetaManager>,
}

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OPERATION: &str = "operation";
const SCHEMA: &str = "schema";
const TB: &str = "tb";
const FIELDS: &str = "fields";

impl AvroConverter {
    pub fn new(meta_manager: Option<RdbMetaManager>) -> Self {
        AvroConverter {
            schema: AvroConverterSchema::get_avro_schema(),
            meta_manager,
        }
    }

    pub async fn row_data_to_avro_key(&mut self, row_data: &RowData) -> Result<String, Error> {
        if let Some(meta_manager) = self.meta_manager.as_mut() {
            let tb_meta = meta_manager
                .get_tb_meta(&row_data.schema, &row_data.tb)
                .await?;
            let convert = |col_values: &HashMap<String, ColValue>| {
                if let Some(col) = &tb_meta.order_col {
                    if let Some(value) = col_values.get(col) {
                        return value.to_option_string();
                    }
                }
                None
            };

            if let Some(key) = match row_data.row_type {
                RowType::Insert => convert(&row_data.after.as_ref().unwrap()),
                RowType::Update | RowType::Delete => convert(&row_data.before.as_ref().unwrap()),
            } {
                return Ok(key);
            }
        }
        Ok(String::new())
    }

    pub fn row_data_to_avro_value(&self, row_data: RowData) -> Result<Vec<u8>, Error> {
        let mut cols = vec![];
        let mut merge_cols = |col_values: &Option<HashMap<String, ColValue>>| {
            if let Some(value) = col_values {
                for key in value.keys() {
                    if !cols.contains(key) {
                        cols.push(key.into())
                    }
                }
            }
        };
        merge_cols(&row_data.before);
        merge_cols(&row_data.after);
        cols.sort();

        // before
        let before = if let Value::Map(v) = Self::col_values_to_avro(&cols, &row_data.before) {
            Value::Union(1, Box::new(Value::Map(v)))
        } else {
            Value::Union(0, Box::new(Value::Null))
        };

        // after
        let after = if let Value::Map(v) = Self::col_values_to_avro(&cols, &row_data.after) {
            Value::Union(1, Box::new(Value::Map(v)))
        } else {
            Value::Union(0, Box::new(Value::Null))
        };

        // fields
        let fields = if cols.is_empty() {
            Value::Union(0, Box::new(Value::Null))
        } else {
            let mut fields = vec![];
            for col in cols {
                fields.push(AvroFieldDef {
                    name: col,
                    type_name: "".into(),
                });
            }
            Value::Union(1, Box::new(apache_avro::to_value(fields).unwrap()))
        };

        let value = Value::Record(vec![
            (SCHEMA.into(), Value::String(row_data.schema.into())),
            (TB.into(), Value::String(row_data.tb.into())),
            (
                OPERATION.into(),
                Value::String(row_data.row_type.to_string()),
            ),
            (FIELDS.into(), fields),
            (BEFORE.into(), before),
            (AFTER.into(), after),
        ]);
        Ok(to_avro_datum(&self.schema, value)?)
    }

    pub fn avro_value_to_row_data(&self, payload: Vec<u8>) -> Result<RowData, Error> {
        let mut reader = payload.as_slice();
        let value = from_avro_datum(&self.schema, &mut reader, None)?;
        let mut avro_map = Self::avro_to_map(value);

        let avro_to_string = |value: Option<Value>| {
            if let Some(v) = value {
                if let Value::String(string_v) = v {
                    return string_v;
                }
            }
            String::new()
        };

        let schema = avro_to_string(avro_map.remove(SCHEMA));
        let tb = avro_to_string(avro_map.remove(TB));
        let operation = avro_to_string(avro_map.remove(OPERATION));
        let _fields = self.avro_to_fields(avro_map.remove(FIELDS));
        let before = self.avro_to_col_values(avro_map.remove(BEFORE));
        let after = self.avro_to_col_values(avro_map.remove(AFTER));

        Ok(RowData {
            schema,
            tb,
            row_type: RowType::from_str(&operation)?,
            before,
            after,
        })
    }

    fn avro_to_fields(&self, value: Option<Value>) -> Vec<AvroFieldDef> {
        if let Some(v) = value {
            return apache_avro::from_value(&v).unwrap();
        }
        vec![]
    }

    fn avro_to_col_values(&self, value: Option<Value>) -> Option<HashMap<String, ColValue>> {
        if value.is_none() {
            return None;
        }

        // Some(Union(1, Map({
        //     "bytes_col": Union(4, Bytes([5, 6, 7, 8])),
        //     "string_col": Union(1, String("string_after")),
        //     "boolean_col": Union(5, Boolean(true)),
        //     "long_col": Union(2, Long(2)),
        //     "null_col": Union(0, Null),
        //     "double_col": Union(3, Double(2.2))
        //   })))

        if let Value::Union(1, v) = value.unwrap() {
            if let Value::Map(map_v) = *v {
                let mut col_values = HashMap::new();
                for (col, value) in map_v {
                    col_values.insert(col.into(), Self::avro_to_col_value(value));
                }
                return Some(col_values);
            }
        }
        None
    }

    fn col_values_to_avro(
        cols: &Vec<String>,
        col_values: &Option<HashMap<String, ColValue>>,
    ) -> Value {
        if cols.is_empty() || col_values.is_none() {
            return Value::Null;
        }

        let mut avro_values = HashMap::new();
        for (col, value) in col_values.as_ref().unwrap() {
            let avro_value = Self::col_value_to_avro(value);
            let union_position = match avro_value {
                Value::Null => 0,
                Value::String(_) => 1,
                Value::Long(_) => 2,
                Value::Double(_) => 3,
                Value::Bytes(_) => 4,
                Value::Boolean(_) => 5,
                // Not supported
                _ => 0,
            };
            avro_values.insert(
                col.into(),
                Value::Union(union_position, Box::new(avro_value)),
            );
        }
        Value::Map(avro_values)
    }

    fn col_value_to_avro(value: &ColValue) -> Value {
        match value {
            ColValue::Tiny(v) => Value::Long(*v as i64),
            ColValue::UnsignedTiny(v) => Value::Long(*v as i64),
            ColValue::Short(v) => Value::Long(*v as i64),
            ColValue::UnsignedShort(v) => Value::Long(*v as i64),
            ColValue::Long(v) => Value::Long(*v as i64),
            ColValue::Year(v) => Value::Long(*v as i64),

            ColValue::UnsignedLong(v) => Value::Long(*v as i64),
            ColValue::LongLong(v) => Value::Long(*v),
            ColValue::Bit(v) => Value::Long(*v as i64),
            ColValue::Set(v) => Value::Long(*v as i64),
            ColValue::Enum(v) => Value::Long(*v as i64),
            // may lose precision
            ColValue::UnsignedLongLong(v) => Value::Long(*v as i64),

            ColValue::Float(v) => Value::Double(*v as f64),
            ColValue::Double(v) => Value::Double(*v),
            ColValue::Blob(v) | ColValue::Json(v) => Value::Bytes(v.clone()),

            ColValue::Decimal(v)
            | ColValue::Time(v)
            | ColValue::Date(v)
            | ColValue::DateTime(v)
            | ColValue::Timestamp(v)
            | ColValue::String(v)
            | ColValue::Set2(v)
            | ColValue::Enum2(v)
            | ColValue::Json2(v) => Value::String(v.clone()),
            ColValue::MongoDoc(v) => Value::String(v.to_string()),

            ColValue::Bool(v) => Value::Boolean(*v),
            ColValue::None => Value::Null,
        }
    }

    fn avro_to_col_value(value: Value) -> ColValue {
        match value {
            Value::Long(v) => ColValue::LongLong(v),
            Value::Double(v) => ColValue::Double(v),
            Value::Bytes(v) => ColValue::Blob(v),
            Value::String(v) => ColValue::String(v),
            Value::Boolean(v) => ColValue::Bool(v),
            Value::Null => ColValue::None,
            Value::Union(_, v) => Self::avro_to_col_value(*v),
            // NOT supported
            _ => ColValue::None,
        }
    }

    fn avro_to_map(value: Value) -> HashMap<String, Value> {
        let mut avro_map = HashMap::new();
        if let Value::Record(record) = value {
            for (field, value) in record {
                avro_map.insert(field, value);
            }
        }
        avro_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRING_COL: &str = "string_col";
    const LONG_COL: &str = "long_col";
    const DOUBLE_COL: &str = "double_col";
    const BYTES_COL: &str = "bytes_col";
    const BOOLEAN_COL: &str = "boolean_col";
    const NULL_COL: &str = "null_col";

    #[test]
    fn test_row_data_to_avro() {
        let schema = "db1";
        let tb = "tb1";

        let mut before = HashMap::new();
        before.insert(STRING_COL.into(), ColValue::String("string_before".into()));
        before.insert(LONG_COL.into(), ColValue::LongLong(1));
        before.insert(DOUBLE_COL.into(), ColValue::Double(1.1));
        before.insert(BYTES_COL.into(), ColValue::Blob(vec![1, 2, 3, 4]));
        before.insert(BOOLEAN_COL.into(), ColValue::Bool(false));
        before.insert(NULL_COL.into(), ColValue::None);

        let mut after = HashMap::new();
        after.insert(STRING_COL.into(), ColValue::String("string_after".into()));
        after.insert(LONG_COL.into(), ColValue::LongLong(2));
        after.insert(DOUBLE_COL.into(), ColValue::Double(2.2));
        after.insert(BYTES_COL.into(), ColValue::Blob(vec![5, 6, 7, 8]));
        after.insert(BOOLEAN_COL.into(), ColValue::Bool(true));
        after.insert(NULL_COL.into(), ColValue::None);

        let avro_converter = AvroConverter::new(None);

        let validate = |row_data: RowData| {
            let payload = avro_converter
                .row_data_to_avro_value(row_data.clone())
                .unwrap();
            let decoded_row_data = avro_converter.avro_value_to_row_data(payload).unwrap();
            assert_eq!(row_data, decoded_row_data);
        };

        let mut row_data = RowData {
            schema: schema.into(),
            tb: tb.into(),
            row_type: RowType::Insert,
            before: None,
            after: Some(after),
        };

        // insert
        validate(row_data.clone());
        // update
        row_data.row_type = RowType::Update;
        row_data.before = Some(before);
        validate(row_data.clone());
        // delete
        row_data.row_type = RowType::Delete;
        row_data.after = None;
        validate(row_data.clone());
    }
}
