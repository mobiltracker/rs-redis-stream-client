use redis::{
    aio::MultiplexedConnection,
    streams::{StreamInfoGroupsReply, StreamReadOptions},
    AsyncCommands, FromRedisValue, RedisError,
};

use crate::{parse_stream_msg, FromStreamMsg, PendingStreamMsg, StreamMsg, StreamMsgPendingCount};

pub struct RedisStreamClient {
    connection: MultiplexedConnection,
    consumer_group: &'static str,
    stream_key: &'static str,
    consumer_name: String,
    options: StreamReadOptions,
}

impl RedisStreamClient {
    pub async fn new(
        mut connection: MultiplexedConnection,
        stream_key: &'static str,
        consumer_group: &'static str,
        consumer_name: &str,
    ) -> Result<RedisStreamClient, redis::RedisError> {
        let xgroup_info: Result<StreamInfoGroupsReply, _> =
            connection.xinfo_groups(stream_key).await;

        let is_created = if xgroup_info.is_err() {
            false
        } else {
            xgroup_info
                .unwrap()
                .groups
                .into_iter()
                .any(|info| info.name == consumer_group)
        };

        if !is_created {
            let _: () = connection
                .xgroup_create_mkstream(stream_key, consumer_group, "$")
                .await?;
        }

        let options = StreamReadOptions::default()
            .group(consumer_group, consumer_name.clone())
            .count(1);
        Ok(Self {
            connection,
            consumer_group,
            stream_key,
            consumer_name: consumer_name.to_string(),
            options,
        })
    }

    pub fn with_no_ack(self) -> RedisStreamClient {
        Self {
            options: self.options.noack(),
            ..self
        }
    }

    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    pub fn with_consumer_name(self, consumer_name: &str) -> RedisStreamClient {
        let options = self.options.group(self.consumer_group, consumer_name);
        Self {
            options,
            consumer_name: consumer_name.to_owned(),
            ..self
        }
    }

    pub async fn read_next_pending_raw(
        &mut self,
        last_msg_id: &str,
    ) -> Result<Option<StreamMsgPendingCount>, redis::RedisError> {
        let data: redis::Value = self
            .connection
            .xread_options(&[self.stream_key], &[last_msg_id], &self.options)
            .await?;

        let parsed = parse_stream_msg(data)?;

        if let Some(parsed) = parsed {
            let msg: redis::Value = self
                .connection
                .xpending_count(
                    &[self.stream_key],
                    &[self.consumer_group],
                    last_msg_id,
                    "+",
                    "1",
                )
                .await
                .unwrap();

            let pending_count = get_pending_counts(msg);

            Ok(Some(StreamMsgPendingCount {
                stream_msg: parsed,
                pending_count,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn read_next_raw(&mut self) -> Result<Option<StreamMsg>, redis::RedisError> {
        let data: redis::Value = self
            .connection
            .xread_options(&[self.stream_key], &[">"], &self.options)
            .await?;

        parse_stream_msg(data)
    }

    pub async fn read_next<T, E>(&mut self) -> Result<Option<T>, redis::RedisError>
    where
        T: FromStreamMsg,
        E: Into<RedisError>,
    {
        let msg = self
            .read_next_raw()
            .await?
            .map(|msg| T::from_stream_msg(msg))
            .transpose()?;

        Ok(msg)
    }

    pub async fn read_next_pending<T>(
        &mut self,
        last_msg_id: &str,
    ) -> Result<Option<PendingStreamMsg<T>>, redis::RedisError>
    where
        T: FromStreamMsg,
    {
        let msg = self.read_next_pending_raw(last_msg_id).await?;
        let pending_count = msg.as_ref().map(|msg| msg.pending_count).unwrap_or(0);

        let msg = msg
            .map(|msg| T::from_stream_msg(msg.stream_msg))
            .transpose()?
            .map(|msg| PendingStreamMsg {
                stream_msg: msg,
                pending_count,
            });

        Ok(msg)
    }

    pub async fn ack_message_id(&mut self, msg_id: &str) -> Result<(), redis::RedisError> {
        self.connection
            .xack(&self.stream_key, self.consumer_group, &[msg_id])
            .await
    }
}

fn get_pending_counts(msg: redis::Value) -> i64 {
    match msg {
        redis::Value::Bulk(bulk) => match bulk.get(0).unwrap() {
            redis::Value::Bulk(bulk) => {
                let retry_count = bulk.get(3).unwrap();
                FromRedisValue::from_redis_value(retry_count).unwrap()
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
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

        let mut stream_client = RedisStreamClient::new(
            conn.clone(),
            test_stream_key,
            "test-group",
            "test-group-consumer",
        )
        .await
        .unwrap();

        let _: () = conn
            .xadd(
                test_stream_key,
                "*",
                &[("key", "value"), ("key2", "value2")],
            )
            .await
            .unwrap();

        let msg = stream_client.read_next_raw().await.unwrap().unwrap();
        println!("{:?}", msg);
        stream_client.ack_message_id(&msg.id).await.unwrap();
        let msg = stream_client.read_next_raw().await.unwrap();
        assert!(msg.is_none())
    }

    #[tokio::test]
    async fn pending_count() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();

        let test_stream_key = "test-stream-key";
        let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();

        let mut stream_client = RedisStreamClient::new(
            conn.clone(),
            test_stream_key,
            "test-group",
            "test-group-consumer",
        )
        .await
        .unwrap();

        let _: () = conn
            .xadd(test_stream_key, "*", &[("name", "foobar 2")])
            .await
            .unwrap();

        let msg = stream_client.read_next_raw().await.unwrap().unwrap();
        let msg_id = msg.id;

        let msg = stream_client.read_next_pending_raw("0").await.unwrap();

        assert_eq!(2, msg.unwrap().pending_count);

        stream_client.ack_message_id(&msg_id).await.unwrap()
    }
}
