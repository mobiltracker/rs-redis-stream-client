use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use redis::{
    streams::{StreamInfoGroupsReply, StreamReadOptions, StreamReadReply},
    Commands, Connection, Value,
};

pub struct RedisStreamClient {
    connection: Arc<Mutex<Connection>>,
    consumer_group: &'static str,
    stream_key: &'static str,
    consumer_key: String,
    options: StreamReadOptions,
}

pub struct RedisStreamMessage {
    pub key: String,
    pub stream_key: &'static str,
    pub group_key: &'static str,
    pub inner_map: HashMap<String, Value>,
    pub connection: Arc<Mutex<Connection>>,
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

    pub fn ack(self) -> Result<(), redis::RedisError> {
        let mut connection = self.connection.lock().unwrap();
        let _: () = connection.xack(self.stream_key, self.group_key, &[self.key])?;
        Ok(())
    }
}

impl RedisStreamClient {
    pub fn new(
        mut connection: Connection,
        stream_key: &'static str,
        consumer_group: &'static str,
        consumer_prefix: &str,
    ) -> Result<RedisStreamClient, redis::RedisError> {
        let xgroup_info: StreamInfoGroupsReply = connection.xinfo_groups(stream_key)?;

        let is_created = xgroup_info
            .groups
            .into_iter()
            .any(|info| info.name == consumer_group);

        if !is_created {
            let _: () = connection.xgroup_create(stream_key, consumer_group, "$")?;
        }

        let stream_consumer_info: redis::streams::StreamInfoConsumersReply =
            connection.xinfo_consumers(stream_key, consumer_group)?;

        let consumer_key = format!(
            "{}-{}",
            consumer_prefix,
            stream_consumer_info.consumers.len()
        );

        let options = StreamReadOptions::default()
            .group(consumer_group, consumer_key.clone())
            .count(1);
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
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

    pub fn ack_message_id(&self, msg_id: &str) -> Result<(), redis::RedisError> {
        self.connection
            .lock()
            .unwrap()
            .xack(&self.consumer_key, self.consumer_group, &[msg_id])
    }

    pub fn read_next(&mut self) -> Result<Option<RedisStreamMessage>, redis::RedisError> {
        let mut data: StreamReadReply = {
            let mut connection = self.connection.lock().unwrap();
            connection.xread_options(&[self.stream_key], &[">"], &self.options)?
        };

        if data.keys.is_empty() {
            return Ok(None);
        }

        let mut msgs = data.keys.pop().expect("infalible").ids;

        if msgs.is_empty() {
            return Ok(None);
        }

        let msg = msgs.pop().expect("infalible");

        Ok(Some(RedisStreamMessage {
            inner_map: msg.map,
            key: msg.id,
            group_key: self.consumer_group,
            stream_key: self.stream_key,
            connection: self.connection.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use redis::Commands;

    use crate::sync::RedisStreamClient;

    #[test]
    fn it_works() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();

        let test_stream_key = "test-stream-key";
        let mut conn = client.get_connection().unwrap();
        let _: () = conn
            .xadd(test_stream_key, "*", &[("key", "value")])
            .unwrap();

        let mut stream_client =
            RedisStreamClient::new(conn, test_stream_key, "test-group", "test-group-consumer")
                .unwrap();

        let msg = stream_client.read_next().unwrap().unwrap();
        println!("{:?}", msg);
        msg.ack().unwrap();
        let msg = stream_client.read_next().unwrap();
        assert!(msg.is_none())
    }
}
