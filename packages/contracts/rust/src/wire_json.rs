//! JSON transports 64-bit counters and nanoseconds as decimal strings.
pub mod i64_string {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Text(String),
            Integer(i64),
        }
        match Value::deserialize(deserializer)? {
            Value::Integer(value) => Ok(value),
            Value::Text(value) => value.parse().map_err(serde::de::Error::custom),
        }
    }
}
pub mod u64_string {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Text(String),
            Integer(u64),
        }
        match Value::deserialize(deserializer)? {
            Value::Integer(value) => Ok(value),
            Value::Text(value) => value.parse().map_err(serde::de::Error::custom),
        }
    }
}
