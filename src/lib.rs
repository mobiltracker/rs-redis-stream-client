use std::collections::HashMap;

use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, Value,
};

pub struct RedisStreamClient<'a, 'b, T: AsyncCommands> {
    connection: &'a mut T,
    consumer_group: &'b str,
    stream_key: &'b str,
    consumer_key: String,
    options: StreamReadOptions,
}

pub struct RedisStreamMessage<'a, T: AsyncCommands> {
    key: String,
    stream_key: &'a str,
    group_key: &'a str,
    inner_map: HashMap<String, Value>,
    connection: &'a mut T,
}

impl<'a, T: AsyncCommands> RedisStreamMessage<'a, T> {
    pub fn get_field(&self, k: &str) -> Option<&Value> {
        self.inner_map.get(k)
    }
    pub async fn ack(self) -> Result<(), redis::RedisError> {
        let _: () = self
            .connection
            .xack(self.stream_key, self.group_key, &[self.key])
            .await?;

        Ok(())
    }
}

impl<'a, 'b, T: AsyncCommands> RedisStreamClient<'a, 'b, T> {
    pub async fn new(
        connection: &'a mut T,
        stream_key: &'b str,
        consumer_group: &'b str,
        consumer_prefix: &'b str,
    ) -> Result<RedisStreamClient<'a, 'b, T>, redis::RedisError> {
        // We do not care if this fails (its going to fail if the group is already created)
        let _: Option<()> = connection
            .xgroup_create(stream_key, consumer_group, "$")
            .await
            .ok();

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

    pub fn with_no_ack(self) -> RedisStreamClient<'a, 'b, T> {
        Self {
            options: self.options.noack(),
            ..self
        }
    }

    pub fn consumer_key(&self) -> &str {
        &self.consumer_key
    }

    pub async fn read_next(
        &'a mut self,
    ) -> Result<Option<RedisStreamMessage<'a, T>>, redis::RedisError> {
        let mut data: StreamReadReply = self
            .connection
            .xread_options(&[self.stream_key], &[">"], &self.options)
            .await?;

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
            connection: self.connection,
            group_key: self.consumer_group,
            stream_key: self.stream_key,
        }))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
