use crate::{ActorID, ChangeRequest, Clock, Operation, OpID};
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/*
impl Serialize for OpID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
      let s = match self {
        OpID::Root => "00000000-0000-0000-0000-000000000000".to_string(),
        OpID::ID(seq,actor) => format!("{}@{}",seq,actor),
      };
      serializer.serialize_str(s.as_str())
    }
}

impl<'de> Deserialize<'de> for OpID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OpIDVisitor;
        impl<'de> Visitor<'de> for OpIDVisitor {
            type Value = OpID;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("`00000000-0000-0000-0000-000000000000` or `$seq@$actor_id`")
            }

            fn visit_str<E>(self, v: &str) -> Result<OpID, E>
            where
                E: Error 
            {
              let err = Error::invalid_value(Unexpected::Str(&v),&"A valid OpID");

              if v == "00000000-0000-0000-0000-000000000000" {
                Ok(OpID::Root)
              } else {
                let mut i = v.split("@");
                match (i.next(),i.next(),i.next()) {
                  (Some(seq_str), Some(actor_str), None) => 
                      if let Ok(seq) = seq_str.parse() {
                        Ok(OpID::ID(seq, actor_str.to_string()))
                      } else {
                        Err(err)
                      }
                  _ => Err(err)
                }
              }
            }
        }
        deserializer.deserialize_str(OpIDVisitor)
    }
}
*/

#[cfg(test)]
mod tests {
    use crate::{ActorID, ChangeRequest, ChangeRequestType, Clock, OpID, Operation};
    use serde_json;

    #[test]
    fn do_tests() {
        let a = OpID::Root;
        let b = OpID::ID(2,"909a8dcd-ad16-431c-8ecd-a9ca1a8dd8c6".to_string());
        let c = serde_json::to_string(&a).unwrap();
        let d = serde_json::to_string(&b).unwrap();
        assert_eq!(c,"\"00000000-0000-0000-0000-000000000000\"".to_string());
        assert_eq!(d,"\"2@909a8dcd-ad16-431c-8ecd-a9ca1a8dd8c6\"".to_string());
        let e : OpID = serde_json::from_str(&c).unwrap();
        let f : OpID = serde_json::from_str(&d).unwrap();
        assert_eq!(a,e);
        assert_eq!(b,f);
    }
}
