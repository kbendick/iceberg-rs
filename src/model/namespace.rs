/*!
A table’s [schema](https://iceberg.apache.org/spec/#schemas-and-data-types) is a list of named columns, represented by [SchemaV2].
All data types are either [primitives](PrimitiveType) or nested types, which are [Map], [List], or [Struct]. A table [SchemaV2] is also a [Struct] type.
*/
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize, Serializer,
};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(remote = "Self")]
/// Primitive Types within a schema.
pub struct Namespace {
    /// levels
    levels: Vec<String>,
}

impl Namespace {
    fn is_empty(&self) -> bool {
        return self.levels.is_empty();
    }
}

impl Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&*self.levels.join("."))
    }
}

/// Serialize for PrimitiveType with special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let this = String::deserialize(deserializer)?;
        // let levels = this.trim().split(".").collect();
        let levels = this.trim().split(".").map(|s| String::from(s)).collect::<Vec<_>>();
        return Ok(Namespace { levels });
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_namespace_from_string() {
        let data = r#"
        {
            "levels": "accounting.tax"
        }
        "#;
        assert!(serde_json::from_str::<Namespace>(&data).is_ok());
        // let data = r#"
        // {
        //     "type" : "anyother"
        // }
        // "#;
        // assert!(serde_json::from_str::<Struct>(data).is_err());
    }

    // #[test]
    // fn test_decimal() {
    //     let data = r#"
    //     {
    //         "id" : 1,
    //         "name": "struct_name",
    //         "required": true,
    //         "field_type": "decimal(1,1)"
    //     }
    //     "#;
    //     let result_struct = serde_json::from_str::<StructField>(data).unwrap();
    //     assert!(matches!(
    //         result_struct.field_type,
    //         AllType::Primitive(PrimitiveType::Decimal {
    //             precision: 1,
    //             scale: 1
    //         })
    //     ));
    //
    //     let invalid_decimal_data = r#"
    //     {
    //         "id" : 1,
    //         "name": "struct_name",
    //         "required": true,
    //         "field_type": "decimal(1,1000)"
    //     }
    //     "#;
    //     assert!(serde_json::from_str::<StructField>(invalid_decimal_data).is_err());
    // }
    //
    // #[test]
    // fn test_boolean() {
    //     let data = r#"
    //     {
    //         "id" : 1,
    //         "name": "struct_name",
    //         "required": true,
    //         "field_type": "boolean"
    //     }
    //     "#;
    //     let result_struct = serde_json::from_str::<StructField>(data).unwrap();
    //     assert!(matches!(
    //         result_struct.field_type,
    //         AllType::Primitive(PrimitiveType::Boolean)
    //     ));
    // }
    //
    // #[test]
    // fn test_fixed() {
    //     let data = r#"
    //     {
    //         "id" : 1,
    //         "name": "struct_name",
    //         "required": true,
    //         "field_type": "fixed[1]"
    //     }
    //     "#;
    //     let result_struct = serde_json::from_str::<StructField>(data).unwrap();
    //     assert!(matches!(
    //         result_struct.field_type,
    //         AllType::Primitive(PrimitiveType::Fixed(1),)
    //     ));
    //
    //     let invalid_fixed_data = r#"
    //     {
    //         "id" : 1,
    //         "name": "struct_name",
    //         "required": true,
    //         "field_type": "fixed[0.1]"
    //     }
    //     "#;
    //     assert!(serde_json::from_str::<StructField>(invalid_fixed_data).is_err());
    // }
    //
    // #[test]
    // fn test_all_valid_types() {
    //     let type_mappings = vec![
    //         PrimitiveType::Boolean,
    //         PrimitiveType::Int,
    //         PrimitiveType::Long,
    //         PrimitiveType::Float,
    //         PrimitiveType::Double,
    //         PrimitiveType::Decimal {
    //             precision: 1,
    //             scale: 2,
    //         },
    //         PrimitiveType::Date,
    //         PrimitiveType::Time,
    //         PrimitiveType::Timestamp,
    //         PrimitiveType::Timestampz,
    //         PrimitiveType::String,
    //         PrimitiveType::Uuid,
    //         PrimitiveType::Fixed(1),
    //         PrimitiveType::Binary,
    //     ];
    //
    //     for primitive in type_mappings {
    //         let sf = StructField {
    //             id: 1,
    //             name: "name".to_string(),
    //             required: true,
    //             field_type: AllType::Primitive(primitive.clone()),
    //             doc: None,
    //         };
    //
    //         let j = serde_json::to_string(&sf).unwrap();
    //         let unserde: StructField = serde_json::from_str(&j).unwrap();
    //         assert_eq!(unserde.field_type, AllType::Primitive(primitive));
    //     }
    // }
    //
    // #[test]
    // fn test_schema() {
    //     let data = r#"
    //     {
    //         "schema-id" : 1,
    //         "type": "struct",
    //         "fields" : [
    //             {
    //                 "id" : 1,
    //                 "name": "struct_name",
    //                 "required": true,
    //                 "field_type": "fixed[1]"
    //             }
    //         ],
    //         "name-mapping": {
    //             "default" : [
    //                 {
    //                     "field-id": 4,
    //                     "names": ["latitude", "lat"]
    //                 }
    //             ]
    //         }
    //     }
    //     "#;
    //     let result_struct = serde_json::from_str::<SchemaV2>(data).unwrap();
    //     assert_eq!(1, result_struct.schema_id);
    //     assert_eq!(None, result_struct.identifier_field_ids);
    //     assert_eq!(1, result_struct.struct_fields.fields.len());
    //     assert_eq!(1, result_struct.name_mapping.unwrap().default.len());
    // }
    //
    // #[test]
    // fn test_list_type() {
    //     let data = r#"
    //             {
    //                 "type": "list",
    //                 "element-id": 3,
    //                 "element-required": true,
    //                 "element": "string"
    //             }
    //     "#;
    //     let result_struct = serde_json::from_str::<List>(data);
    //     let result_struct = result_struct.unwrap();
    //     assert_eq!(3, result_struct.element_id);
    //     assert!(result_struct.element_required);
    //     assert_eq!(
    //         AllType::Primitive(PrimitiveType::String),
    //         *result_struct.element
    //     );
    // }
    //
    // #[test]
    // fn test_map_type() {
    //     let data = r#"
    //     {
    //         "type": "map",
    //         "key-id": 4,
    //         "key": "string",
    //         "value-id": 5,
    //         "value-required": false,
    //         "value": "double"
    //     }
    //     "#;
    //     let result_struct = serde_json::from_str::<Map>(data);
    //     let result_struct = result_struct.unwrap();
    //     assert_eq!(4, result_struct.key_id);
    //     assert!(!result_struct.value_required);
    //     assert_eq!(
    //         AllType::Primitive(PrimitiveType::Double),
    //         *result_struct.value
    //     );
    //     assert_eq!(
    //         AllType::Primitive(PrimitiveType::String),
    //         *result_struct.key
    //     );
    // }
    //
    // #[test]
    // fn test_name_mapping() {
    //     let data = r#"
    //     { "field-id": 3, "names": ["location"], "fields": [
    //         { "field-id": 4, "names": ["latitude", "lat"] },
    //         { "field-id": 5, "names": ["longitude", "long"] }
    //     ] }
    //     "#;
    //
    //     let name_mapping: NameMapping = serde_json::from_str(data).unwrap();
    //     assert_eq!(Some(3), name_mapping.field_id);
    //     assert!(name_mapping.fields.is_some())
    // }
}
