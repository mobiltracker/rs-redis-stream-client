use redis::{
    aio::MultiplexedConnection,
    streams::{StreamInfoGroupsReply, StreamReadOptions},
    AsyncCommands,
};

use crate::{parse_stream_msg, StreamMsg};

pub struct RedisStreamClient {
    connection: MultiplexedConnection,
    consumer_group: &'static str,
    stream_key: &'static str,
    consumer_key: String,
    options: StreamReadOptions,
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

    pub async fn read_next(&mut self) -> Result<Option<StreamMsg>, redis::RedisError> {
        let data: redis::Value = self
            .connection
            .xread_options(&[self.stream_key], &[">"], &self.options)
            .await?;

        parse_stream_msg(data)
    }
    pub async fn ack_message_id(&mut self, msg_id: &str) -> Result<(), redis::RedisError> {
        self.connection
            .xack(&self.consumer_key, self.consumer_group, &[msg_id])
            .await
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
        let _: () = conn
            .xadd(
                test_stream_key,
                "*",
                &[("key", "value"), ("key2", "value2")],
            )
            .await
            .unwrap();

        let mut stream_client =
            RedisStreamClient::new(conn, test_stream_key, "test-group", "test-group-consumer")
                .await
                .unwrap();

        let msg = stream_client.read_next().await.unwrap().unwrap();
        println!("{:?}", msg);
        stream_client.ack_message_id(&msg.id).await.unwrap();
        let msg = stream_client.read_next().await.unwrap();
        assert!(msg.is_none())
    }
}
