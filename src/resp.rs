use bytes::Bytes;

/// RESP Value enum representing different Redis data types
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<Value>),
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_creation() {
        let simple = Value::SimpleString("OK".to_string());
        assert_eq!(simple, Value::SimpleString("OK".to_string()));

        let bulk = Value::BulkString(Bytes::from("hello"));
        assert_eq!(bulk, Value::BulkString(Bytes::from("hello")));
    }
}