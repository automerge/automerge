use std::fmt;

use serde::{
    de,
    de::{MapAccess, Unexpected, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::{MapType, ObjectId, RootDiff};

impl Serialize for RootDiff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("objectId", &ObjectId::Root)?;
        map.serialize_entry("type", &MapType::Map)?;
        map.serialize_entry("props", &self.props)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for RootDiff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "camelCase")]
        enum Field {
            ObjectId,
            #[serde(rename = "type")]
            ObjectType,
            Props,
        }

        struct RootDiffVisitor;

        const FIELDS: &[&str] = &["objectId", "type", "props"];
        impl<'de> Visitor<'de> for RootDiffVisitor {
            type Value = RootDiff;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct RootDiff")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut object_id = None;
                let mut object_type = None;
                let mut props = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::ObjectId => {
                            if object_id.is_some() {
                                return Err(de::Error::duplicate_field("objectId"));
                            }
                            object_id = Some(map.next_value()?);
                        }
                        Field::ObjectType => {
                            if object_type.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            object_type = Some(map.next_value()?)
                        }
                        Field::Props => {
                            if props.is_some() {
                                return Err(de::Error::duplicate_field("props"));
                            }
                            props = Some(map.next_value()?)
                        }
                    }
                }

                let object_id: ObjectId =
                    object_id.ok_or_else(|| de::Error::missing_field("objectId"))?;
                let object_type: MapType =
                    object_type.ok_or_else(|| de::Error::missing_field("type"))?;
                let props = props.ok_or_else(|| de::Error::missing_field("props"))?;

                if let ObjectId::Id(opid) = object_id {
                    return Err(de::Error::invalid_value(
                        Unexpected::Str(&opid.to_string()),
                        &"_root",
                    ));
                }
                if object_type != MapType::Map {
                    // currently only two types of map so must be a table
                    return Err(de::Error::invalid_value(Unexpected::Str("table"), &"map"));
                }

                Ok(RootDiff { props })
            }
        }

        deserializer.deserialize_struct("RootDiff", FIELDS, RootDiffVisitor)
    }
}
