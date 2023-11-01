use apache_avro::Schema;

pub struct AvroConverterSchema {}

const SCHEMA_STR: &str = r#"
{
    "type": "record",
    "name": "AvroData",
    "fields": [
        {
            "name": "schema",
            "type": "string",
            "default": ""
        },
        {
            "name": "tb",
            "type": "string",
            "default": ""
        },
        {
            "name": "operation",
            "type": "string",
            "default": ""
        },
        {
            "name": "fields",
            "default": null,
            "type": 
            [
                "null",
                {
                    "type": "array",
                    "items": {
                        "name": "AvroFieldDef",
                        "type": "record",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "type_name",
                                "type": "string",
                                "default": "string"
                            }
                        ]
                    }
                }
            ]
        },
        {
            "name": "before",
            "default": null,
            "type": 
            {
                "type": 
                [
                    "null",
                    {
                        "type": "map",
                        "values": 
                        [
                            "null",
                            "string",
                            "long",
                            "double",
                            "bytes",
                            "boolean"
                        ]
                    }
                ]
            }
        },
        {
            "name": "after",
            "default": null,
            "type": 
            {
                "type": 
                [
                    "null",
                    {
                        "type": "map",
                        "values": 
                        [
                            "null",
                            "string",
                            "long",
                            "double",
                            "bytes",
                            "boolean"
                        ]
                    }
                ]
            }
        }
    ]
}"#;

impl AvroConverterSchema {
    pub fn get_avro_schema() -> Schema {
        Schema::parse_str(SCHEMA_STR).unwrap()
    }
}

/// these structs are generated from avro schema by tool: https://github.com/lerouxrgd/rsgen-avro
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct AvroFieldDef {
    pub name: String,
    #[serde(default = "default_avrofielddef_type_name")]
    pub type_name: String,
}

#[inline(always)]
fn default_avrofielddef_type_name() -> String {
    "".to_owned()
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub enum AvroFieldValue {
    String(String),
    Long(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
struct AvroData {
    pub schema: String,
    pub tb: String,
    pub operation: String,
    #[serde(default = "default_avrodata_fields")]
    pub fields: Option<Vec<AvroFieldDef>>,
    #[serde(default = "default_avrodata_before")]
    pub before: Option<Vec<Option<AvroFieldValue>>>,
    #[serde(default = "default_avrodata_after")]
    pub after: Option<Vec<Option<AvroFieldValue>>>,
}

#[inline(always)]
fn default_avrodata_fields() -> Option<Vec<AvroFieldDef>> {
    None
}

#[inline(always)]
fn default_avrodata_before() -> Option<Vec<Option<AvroFieldValue>>> {
    None
}

#[inline(always)]
fn default_avrodata_after() -> Option<Vec<Option<AvroFieldValue>>> {
    None
}
