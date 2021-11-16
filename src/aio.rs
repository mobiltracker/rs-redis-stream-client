use std::collections::HashMap;

use redis::{
    aio::MultiplexedConnection,
    streams::{StreamInfoGroupsReply, StreamReadOptions, StreamReadReply},
    AsyncCommands, Value,
};

pub struct RedisStreamClient {
    connection: MultiplexedConnection,
    consumer_group: &'static str,
    stream_key: &'static str,
    consumer_key: String,
    options: StreamReadOptions,
}

pub struct RedisStreamMessage {
    key: String,
    stream_key: &'static str,
    group_key: &'static str,
    inner_map: HashMap<String, Value>,
    connection: MultiplexedConnection,
}

impl std::fmt::Debug for RedisStreamMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStreamMessage")
            .field("key", &self.key)
            .field("stream_key", &self.stream_key)
            .field("group_key", &self.group_key)
            .field("inner_map", &self.inner_map)
            .finish()
    }
}

impl RedisStreamMessage {
    pub fn get_field(&self, k: &str) -> Option<&Value> {
        self.inner_map.get(k)
    }
    pub async fn ack(mut self) -> Result<(), redis::RedisError> {
        let _: () = self
            .connection
            .xack(self.stream_key, self.group_key, &[self.key])
            .await?;

        Ok(())
    }
}

impl RedisStreamClient {
    pub async fn new(
        mut connection: MultiplexedConnection,
        stream_key: &'static str,
        consumer_group: &'static str,
        consumer_prefix: &str,
    ) -> Result<RedisStreamClient, redis::RedisError> {
        let xgroup_info: StreamInfoGroupsReply = connection.xinfo_groups(stream_key).await?;

        let is_created = xgroup_info
            .groups
            .into_iter()
            .any(|info| info.name == consumer_group);

        if !is_created {
            let _: () = connection
                .xgroup_create(stream_key, consumer_group, "$")
                .await?;
        }

        let stream_consumer_info: redis::streams::StreamInfoConsumersReply = connection
            .xinfo_consumers(stream_key, consumer_group)
            .await?;

        let consumer_key = format!(
            "{}-{}",
            consumer_prefix,
            stream_consumer_info.consumers.len()
        );

        let options = StreamReadOptions::default()
            .group(consumer_group, consumer_key.clone())
            .count(1);
        Ok(Self {
            connection,
            consumer_group,
            stream_key,
            consumer_key,
            options,
        })
    }

    pub fn with_no_ack(self) -> RedisStreamClient {
        Self {
            options: self.options.noack(),
            ..self
        }
    }

    pub fn consumer_key(&self) -> &str {
        &self.consumer_key
    }

    pub async fn read_next(&mut self) -> Result<Option<RedisStreamMessage>, redis::RedisError> {
        let mut data: redis::Value = self
            .connection
            .xread_options(&[self.stream_key], &[">"], &self.options)
            .await?;

        let msg = parse_stream_msg(data).unwrap().unwrap();

        todo!();

        // if data.keys.is_empty() {
        //     return Ok(None);
        // }

        // let mut msgs = data.keys.pop().expect("infalible").ids;

        // if msgs.is_empty() {
        //     return Ok(None);
        // }

        // let msg = msgs.pop().expect("infalible");

        // Ok(Some(RedisStreamMessage {
        //     inner_map: msg.map,
        //     key: msg.id,
        //     connection: self.connection.clone(),
        //     group_key: self.consumer_group,
        //     stream_key: self.stream_key,
        // }))
    }
}

#[derive(Debug, Default)]
pub struct StreamMsg {
    stream_key: String,
    id: String,
    data: Vec<Vec<String>>,
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

fn parse_stream_msg(data: Value) -> Result<Option<StreamMsg>, redis::RedisError> {
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
                                        .map(|v| match v {
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
                                _ => return redis_stream_err!("Invalid data type for first_msg"),
                            }
                        }
                        _ => return redis_stream_err!("Invalid data type for msg_data"),
                    }
                }
                _ => return redis_stream_err!("Invalid data type for first bulk read"),
            }
        }
        other => return redis_stream_err!(format!("Invalid data: {:?}", other)),
    }

    return Ok(Some(result));
}

#[cfg(test)]
mod tests {
    use redis::AsyncCommands;

    use crate::aio::RedisStreamClient;

    #[tokio::test]
    async fn it_works() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();

        let test_stream_key = "test-stream-key";
        let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();
        let _: () = conn
            .xadd(test_stream_key, "*", &[("key", "value")])
            .await
            .unwrap();

        let mut stream_client =
            RedisStreamClient::new(conn, test_stream_key, "test-group", "test-group-consumer")
                .await
                .unwrap();

        let msg = stream_client.read_next().await.unwrap().unwrap();
        println!("{:?}", msg);
        msg.ack().await.unwrap();
        let msg = stream_client.read_next().await.unwrap();
        assert!(msg.is_none())
    }
}
