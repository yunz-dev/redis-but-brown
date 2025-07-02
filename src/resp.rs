use bytes::{Buf, Bytes};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Incomplete data")]
    Incomplete,
    #[error("Invalid format")]
    InvalidFormat,
}

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

pub fn parse_value(buf: &mut impl Buf) -> Result<Value, ParseError> {
    if !buf.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    match buf.get_u8() {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => Err(ParseError::InvalidFormat),
    }
}

fn parse_simple_string(buf: &mut impl Buf) -> Result<Value, ParseError> {
    let line = read_line(buf)?;
    Ok(Value::SimpleString(line))
}

fn parse_error(buf: &mut impl Buf) -> Result<Value, ParseError> {
    let line = read_line(buf)?;
    Ok(Value::Error(line))
}

fn parse_integer(buf: &mut impl Buf) -> Result<Value, ParseError> {
    let line = read_line(buf)?;
    let num: i64 = line.parse().map_err(|_| ParseError::InvalidFormat)?;
    Ok(Value::Integer(num))
}

fn parse_bulk_string(buf: &mut impl Buf) -> Result<Value, ParseError> {
    let len_line = read_line(buf)?;
    if len_line == "-1" {
        return Ok(Value::Null);
    }
    let len: usize = len_line.parse().map_err(|_| ParseError::InvalidFormat)?;
    if buf.remaining() < len + 2 {
        return Err(ParseError::Incomplete);
    }
    let data = buf.copy_to_bytes(len);
    // consume \r\n
    buf.advance(2);
    Ok(Value::BulkString(data))
}

fn parse_array(buf: &mut impl Buf) -> Result<Value, ParseError> {
    let len_line = read_line(buf)?;
    if len_line == "-1" {
        return Ok(Value::Null);
    }
    let len: usize = len_line.parse().map_err(|_| ParseError::InvalidFormat)?;
    let mut array = Vec::with_capacity(len);
    for _ in 0..len {
        array.push(parse_value(buf)?);
    }
    Ok(Value::Array(array))
}

fn read_line(buf: &mut impl Buf) -> Result<String, ParseError> {
    let mut line = Vec::new();
    loop {
        if !buf.has_remaining() {
            return Err(ParseError::Incomplete);
        }
        let byte = buf.get_u8();
        if byte == b'\r' {
            if !buf.has_remaining() || buf.get_u8() != b'\n' {
                return Err(ParseError::InvalidFormat);
            }
            break;
        }
        line.push(byte);
    }
    String::from_utf8(line).map_err(|_| ParseError::InvalidFormat)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_value_creation() {
        let simple = Value::SimpleString("OK".to_string());
        assert_eq!(simple, Value::SimpleString("OK".to_string()));

        let bulk = Value::BulkString(Bytes::from("hello"));
        assert_eq!(bulk, Value::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR something\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::Error("ERR something".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":123\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::Integer(123));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::Array(vec![
            Value::BulkString(Bytes::from("foo")),
            Value::BulkString(Bytes::from("bar")),
        ]));
    }

    #[test]
    fn test_parse_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let value = parse_value(&mut buf).unwrap();
        assert_eq!(value, Value::Null);
    }
}