use std::fmt::Debug;

use redis::{RedisError, Value};

pub mod aio;
pub mod sync;

#[derive(Debug, Default)]
pub struct StreamMsg {
    pub stream_key: String,
    pub id: String,
    pub data: Vec<String>,
}

pub trait FromStreamMsg<Err: Into<RedisError>> {
    fn from_stream_msg(data: StreamMsg) -> Result<Self, Err>
    where
        Self: Sized;
}

macro_rules! redis_stream_err {
    () => {};
    ($msg: expr) => {
        Err(redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            $msg,
        )))
    };
}

pub fn parse_stream_msg(data: Value) -> Result<Option<StreamMsg>, redis::RedisError> {
    let mut result = StreamMsg::default();
    match data {
        Value::Bulk(values) => {
            let v = values.into_iter().next();
            if v.is_none() {
                return Ok(None);
            }
            match v.unwrap() {
                Value::Bulk(v) => {
                    let mut vals_iter = v.into_iter();
                    let stream_key = match vals_iter.next() {
                        Some(Value::Data(d)) => String::from_utf8(d).unwrap(),
                        _ => return redis_stream_err!("Missing stream key from msg"),
                    };

                    result.stream_key = stream_key;

                    let msg_data = vals_iter.next();
                    match msg_data {
                        Some(Value::Bulk(data)) => {
                            let first_msg = data.into_iter().next();
                            match first_msg {
                                Some(Value::Bulk(msg)) => {
                                    let mut msg_iter = msg.into_iter();

                                    let msg_id = match msg_iter.next() {
                                        Some(Value::Data(d)) => String::from_utf8(d).unwrap(),
                                        _ => {
                                            return redis_stream_err!(
                                                "Missing message if from message"
                                            )
                                        }
                                    };

                                    let key_vals = msg_iter
                                        .flat_map(|v| match v {
                                            Value::Bulk(b) => b
                                                .into_iter()
                                                .map(|v| match v {
                                                    Value::Data(data) => {
                                                        String::from_utf8(data).unwrap()
                                                    }
                                                    // TODO: REsult here
                                                    _ => panic!("Invalid data type for key_vals"),
                                                })
                                                .collect::<Vec<_>>(),
                                            _ => {
                                                panic!("Invalid data type for key_vals")
                                            }
                                        })
                                        .collect::<Vec<_>>();
                                    result.id = msg_id;
                                    result.data = key_vals;
                                }
                                Some(Value::Nil) => return Ok(None),
                                val => {
                                    return redis_stream_err!(format!(
                                        "Invalid data type for first_msg: {:?}",
                                        val
                                    ))
                                }
                            }
                        }
                        _ => return redis_stream_err!("Invalid data type for msg_data"),
                    }
                }
                _ => return redis_stream_err!("Invalid data type for first bulk read"),
            }
        }
        Value::Nil => return Ok(None),
        other => return redis_stream_err!(format!("Invalid data format: {:?}", other)),
    }

    Ok(Some(result))
}
