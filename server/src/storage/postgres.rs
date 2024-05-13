use std::{cmp, iter::empty, num::TryFromIntError, pin::pin, sync::Arc};

use bitvec::{order::Lsb0, vec::BitVec};
use chrono::{DateTime, Utc};
use comet_common::{
    error::ResponsiveError,
    protocol::response::{Response, ReturnCode},
    types::InitialPosition,
};
use deadpool_postgres::GenericClient;
use futures::{Future, TryStreamExt};
use snafu::{Location, ResultExt, Snafu};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;

use crate::storage::{cache::SubscriptionStorageCache, mem::MemorySubscriptionStorage};

use super::{
    mem::{self, MemoryTopicStorage},
    PublishedMessage, SubscriptionStorage, TopicMessage, TopicStorage,
};

const MAX_READ_BITS: usize = 8 * 1024 * 1024 * 100;
const INIT_LOAD_MESSAGES_NUM: usize = 1000;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("memory error"), context(false))]
    Memory {
        source: mem::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("postgres pool config error"))]
    PoolConfig {
        #[snafu(source)]
        error: deadpool_postgres::ConfigError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("postgres pool build error"))]
    PoolBuild {
        #[snafu(source)]
        error: deadpool_postgres::BuildError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("postgres pool error"))]
    Pool {
        #[snafu(source)]
        error: deadpool_postgres::PoolError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("postgres database error"))]
    DataBase {
        #[snafu(source)]
        error: tokio_postgres::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("number overflow"))]
    NumberOverflow {
        #[snafu(source)]
        error: TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> comet_common::protocol::response::Response {
        match self {
            Error::Memory { source, .. } => source.as_response(),
            Error::PoolConfig { error, .. } => {
                Response::new(ReturnCode::Internal, error.to_string())
            }
            Error::PoolBuild { error, .. } => {
                Response::new(ReturnCode::Internal, error.to_string())
            }
            Error::Pool { error, .. } => Response::new(ReturnCode::Internal, error.to_string()),
            Error::DataBase { error, .. } => Response::new(ReturnCode::Internal, error.to_string()),
            Error::NumberOverflow { error, .. } => {
                Response::new(ReturnCode::Internal, error.to_string())
            }
        }
    }
}

pub struct PostgresTopicMessage {
    pub message_id: u64,
    pub topic_name: String,
    pub producer_name: String,
    pub sequence_id: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub payload: Vec<u8>,
}

impl TryFrom<tokio_postgres::Row> for PostgresTopicMessage {
    type Error = tokio_postgres::Error;

    fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: row.try_get::<'_, _, i64>("message_id")? as u64,
            topic_name: row.try_get("topic_name")?,
            producer_name: row.try_get("producer_name")?,
            sequence_id: row.try_get::<'_, _, i64>("sequence_id")? as u64,
            publish_time: row.try_get("publish_time")?,
            deliver_time: row.try_get("deliver_time")?,
            handle_time: row.try_get("handle_time")?,
            payload: row.try_get("payload")?,
        })
    }
}

impl From<PostgresTopicMessage> for TopicMessage {
    fn from(message: PostgresTopicMessage) -> Self {
        Self {
            message_id: message.message_id,
            topic_name: message.topic_name,
            producer_name: message.producer_name,
            sequence_id: message.sequence_id,
            publish_time: message.publish_time,
            deliver_time: message.deliver_time,
            handle_time: message.handle_time,
            payload: message.payload.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
    pub mem_messages_num_limit: Option<usize>,
}

#[derive(Clone)]
pub struct PostgresTopicStorage {
    topic_name: String,
    table_name: String,
    config: Config,
    pool: deadpool_postgres::Pool,
    mem: MemoryTopicStorage,
}

impl PostgresTopicStorage {
    #[tracing::instrument]
    pub async fn new(topic_name: &str, config: Config) -> Result<Self, Error> {
        let mut pg_config = tokio_postgres::Config::new();
        pg_config.host(&config.host);
        pg_config.port(config.port);
        pg_config.user(&config.user);
        pg_config.password(&config.password);
        pg_config.dbname(&config.db_name);
        let mgr_config = deadpool_postgres::ManagerConfig::default();
        let mgr = deadpool_postgres::Manager::from_config(pg_config, NoTls, mgr_config);
        let pool = deadpool_postgres::Pool::builder(mgr)
            .build()
            .context(PoolBuildSnafu)?;
        let mem = MemoryTopicStorage::new();

        let table_name = format!("comet_topic_{topic_name}");
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                message_id      BIGSERIAL PRIMARY KEY,\n
                producer_name   TEXT    NOT NULL,\n
                sequence_id     BIGINT NOT NULL,\n
                publish_time    TIMESTAMP WITH TIME ZONE NOT NULL,\n
                handle_time     TIMESTAMP WITH TIME ZONE NOT NULL,\n
                deliver_time    TIMESTAMP WITH TIME ZONE,\n
                payload         BYTEA    NOT NULL,\n
                create_time     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,\n
                UNIQUE (producer_name, sequence_id)
            );",
        );

        query(&pool, |conn| async move {
            conn.execute(&create_table_sql, &[])
                .await
                .context(DataBaseSnafu)
        })
        .await?;

        // 向内存中加载一些数据
        let fetch_sql = format!(
            "SELECT
                message_id,
                '{}' AS topic_name,
                producer_name,
                sequence_id,
                publish_time,
                handle_time,
                deliver_time,
                payload
            FROM 
                {table_name}
            ORDER BY
                message_id DESC
            LIMIT {};",
            topic_name, INIT_LOAD_MESSAGES_NUM
        );

        query(&pool, {
            let mem = mem.clone();
            |conn| async move {
                let mut rows = conn
                    .query_raw(&fetch_sql, empty::<bool>())
                    .await
                    .context(DataBaseSnafu)?;

                let mut rows = pin!(rows);
                while let Some(row) = rows.try_next().await.context(DataBaseSnafu)? {
                    let message =
                        TryInto::<PostgresTopicMessage>::try_into(row).context(DataBaseSnafu)?;
                    mem.push_message(message.into()).await;
                }

                Ok(())
            }
        })
        .await?;

        Ok(Self {
            pool,
            mem,
            topic_name: topic_name.to_owned(),
            table_name,
            config,
        })
    }

    async fn query<F, Fut, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(deadpool_postgres::Object) -> Fut,
        Fut: Future<Output = Result<R, Error>>,
    {
        query(&self.pool, f).await
    }
}

impl TopicStorage for PostgresTopicStorage {
    type Error = Error;

    type SubscriptionStorage = PostgresSubscriptionStorage;

    #[tracing::instrument(skip(self))]
    async fn create_subscription(
        &self,
        subscription_name: &str,
        initial_position: InitialPosition,
    ) -> Result<Self::SubscriptionStorage, Self::Error> {
        PostgresSubscriptionStorage::new(
            self.topic_name.clone(),
            self.table_name.clone(),
            subscription_name.to_owned(),
            initial_position,
            self.pool.clone(),
            self.mem.clone(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn publish_message(&self, message: PublishedMessage) -> Result<u64, Self::Error> {
        let insert_sql = format!(
            "INSERT INTO {} (
                producer_name, 
                sequence_id,
                publish_time,
                handle_time,
                deliver_time,
                payload
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            ) RETURNING message_id;",
            self.table_name,
        );

        let message_id = self
            .query({
                let message = message.clone();
                move |conn| async move {
                    let sequence_id =
                        i64::try_from(message.sequence_id).context(NumberOverflowSnafu)?;
                    let message_id: i64 = conn
                        .query_one(
                            &insert_sql,
                            &[
                                &message.producer_name,
                                &sequence_id,
                                &message.publish_time,
                                &message.handle_time,
                                &message.deliver_time,
                                &message.payload.to_vec(),
                            ],
                        )
                        .await
                        .context(DataBaseSnafu)?
                        .try_get("message_id")
                        .context(DataBaseSnafu)?;
                    Ok(message_id as u64)
                }
            })
            .await?;

        // 添加到内存
        self.mem
            .push_message(TopicMessage::from_publish_message(message, message_id))
            .await;
        if let Some(num_limit) = self.config.mem_messages_num_limit {
            self.mem.shrink(num_limit).await;
        }

        Ok(message_id)
    }

    #[tracing::instrument(skip(self))]
    async fn retain_messages(&self, min_message_id: Option<u64>) -> Result<(), Self::Error> {
        match min_message_id {
            Some(min_id) => {
                let delete_sql = format!("DELETE FROM {} WHERE message_id < $1;", self.table_name);
                self.query(|conn| async move {
                    let message_id = i64::try_from(min_id).context(NumberOverflowSnafu)?;
                    conn.execute(&delete_sql, &[&message_id])
                        .await
                        .context(DataBaseSnafu)?;
                    Ok(())
                })
                .await?;
            }
            None => {
                let delete_sql = format!("DELETE FROM {};", self.table_name);
                self.query(|conn| async move {
                    conn.execute(&delete_sql, &[])
                        .await
                        .context(DataBaseSnafu)?;
                    Ok(())
                })
                .await?;
            }
        }

        // 删除内存
        self.mem.retain_messages(min_message_id).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn drop(&self) -> Result<(), Self::Error> {
        let drop_sql = format!("DROP TABLE {}", self.table_name);
        self.query(|conn| async move {
            conn.execute(&drop_sql, &[]).await.context(DataBaseSnafu)?;
            Ok(())
        })
        .await?;
        // 内存
        self.mem.drop().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn last_sequence_id(&self, producer_name: &str) -> Result<u64, Self::Error> {
        let fetch_sql = format!(
            "SELECT sequence_id
            FROM {}
            WHERE producer_name = $1
            ORDER BY sequence_id DESC
            LIMIT 1;",
            self.table_name
        );
        let sequence_id = self
            .query({
                let producer_name = producer_name.to_owned();
                |conn| async move {
                    conn.query_opt(&fetch_sql, &[&producer_name])
                        .await
                        .context(DataBaseSnafu)?
                        .map(|row| row.try_get::<'_, _, i64>("sequence_id"))
                        .transpose()
                        .context(DataBaseSnafu)
                }
            })
            .await?;
        Ok(sequence_id.map(|id| id as u64).unwrap_or_default())
    }
}

#[derive(Clone)]
pub struct PostgresSubscriptionStorage {
    topic_name: String,
    topic_table_name: String,
    table_name: String,
    pool: deadpool_postgres::Pool,
    cache: Arc<RwLock<super::cache::SubscriptionStorageCache>>,
}

impl PostgresSubscriptionStorage {
    #[tracing::instrument(skip(pool, topic_storage))]
    async fn new(
        topic_name: String,
        topic_table_name: String,
        subscription_name: String,
        initial_position: InitialPosition,
        pool: deadpool_postgres::Pool,
        topic_storage: MemoryTopicStorage,
    ) -> Result<Self, Error> {
        let table_name = format!("comet_sub_{}_{}", topic_name, subscription_name);

        query(&pool, {
            let table_name = table_name.clone();
            let topic_table_name = topic_table_name.clone();
            |mut conn| async move {
                let txn = conn.transaction().await.context(DataBaseSnafu)?;

                // 判断表是否存在
                let check_sql = format!(
                    "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename='{table_name}');",
                );
                let exists = txn
                    .query_one(&check_sql, &[])
                    .await
                    .context(DataBaseSnafu)?
                    .try_get("exists")
                    .context(DataBaseSnafu)?;
                if exists {
                    return Ok(());
                }
                // 创建 subscription 表
                let create_table_sql = format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (\n
                    message_id      BIGINT PRIMARY KEY,\n
                    loaded          BOOL   DEFAULT FALSE,\n
                    read_by         TEXT   DEFAULT NULL,\n
                    acked           BOOL   DEFAULT FALSE,\n
                    create_time     TIMESTAMP WITH TIME ZONE NOT NULL,\n
                    deliver_time    TIMESTAMP WITH TIME ZONE\n
                );"
                );
                txn.execute(&create_table_sql, &[])
                    .await
                    .context(DataBaseSnafu)?;

                // 生成记录
                if matches!(initial_position, InitialPosition::Earliest) {
                    let insert_sql = format!(
                        "INSERT INTO {table_name} \
                        (message_id, create_time, deliver_time) \
                        SELECT message_id, create_time, deliver_time \
                        FROM {topic_table_name}",
                    );
                    txn.execute(&insert_sql, &[]).await.context(DataBaseSnafu)?;
                }

                // 创建索引
                let create_time_idx_sql =
                    format!("CREATE INDEX idx_create_time ON {table_name} (create_time);");
                let deliver_time_idx_sql =
                    format!("CREATE INDEX idx_deliver_time ON {table_name} (deliver_time);");
                // 创建索引
                txn.batch_execute(&format!(
                    "{create_time_idx_sql}\
                    {deliver_time_idx_sql}",
                ))
                .await
                .context(DataBaseSnafu)?;

                txn.commit().await.context(DataBaseSnafu)?;
                Ok(())
            }
        })
        .await?;

        let init_db_start_id: Option<u64> = {
            let fetch_sql = format!(
                "SELECT message_id FROM {} WHERE read_by IS NULL ORDER BY message_id LIMIT 1;",
                table_name
            );
            query(&pool, |conn| async move {
                let id = conn
                    .query_opt(&fetch_sql, &[])
                    .await
                    .context(DataBaseSnafu)?
                    .map(|row| row.try_get::<'_, _, i64>("message_id"))
                    .transpose()
                    .context(DataBaseSnafu)?;
                Ok(id.map(|id| id as u64))
            })
            .await?
        };
        let reads = match init_db_start_id {
            Some(init_start_id) => {
                let fetch_sql = format!(
                    "SELECT
                        message_id
                    FROM
                        {table_name}
                    WHERE
                        read_by IS NOT NULL
                        AND message_id >= $1
                    ;",
                );
                query(&pool, |conn| async move {
                    let mut reads = BitVec::<u64, Lsb0>::new();
                    let mut rows = conn
                        .query_raw(&fetch_sql, &[&(init_start_id as i64)])
                        .await
                        .context(DataBaseSnafu)?;
                    let mut rows = pin!(rows);
                    while let Some(row) = rows.try_next().await.context(DataBaseSnafu)? {
                        let message_id: i64 = row.try_get("message_id").context(DataBaseSnafu)?;
                        let index = (message_id as u64 - init_start_id) as usize;
                        if index >= reads.len() {
                            reads.resize(index + 10, false);
                        }
                        if let Some(mut bit) = reads.get_mut(index) {
                            bit.set(true);
                        }
                    }
                    Ok(reads)
                })
                .await?
            }
            None => BitVec::<u64, Lsb0>::new(),
        };

        Ok(Self {
            topic_table_name: topic_table_name.to_owned(),
            pool: pool.clone(),
            topic_name: topic_name.to_owned(),
            table_name: table_name.clone(),
            cache: Arc::new(RwLock::new(SubscriptionStorageCache {
                init_db_start_unread_id: init_db_start_id,
                mem: MemorySubscriptionStorage::new(topic_storage),
                reads,
            })),
        })
    }

    async fn query<F, Fut, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(deadpool_postgres::Object) -> Fut,
        Fut: Future<Output = Result<R, Error>>,
    {
        query(&self.pool, f).await
    }
}

impl SubscriptionStorage for PostgresSubscriptionStorage {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    async fn add_record(&self, message_id: u64) -> Result<(), Self::Error> {
        let insert_sql = format!(
            "INSERT INTO {}
            (message_id, create_time)
            SELECT 
                $1 AS message_id,
                create_time
            FROM {}
            WHERE
                message_id = $1;",
            self.table_name, self.topic_table_name
        );
        self.query(|conn| async move {
            let message_id = i64::try_from(message_id).context(NumberOverflowSnafu)?;
            conn.execute(&insert_sql, &[&message_id])
                .await
                .context(DataBaseSnafu)?;
            Ok(())
        })
        .await?;
        // 内存
        let mut cache = self.cache.write().await;
        cache.mem.add_record(message_id).await?;
        let init_id = *cache.init_db_start_unread_id.get_or_insert(message_id);
        let index = (message_id - init_id) as usize;
        if index >= cache.reads.len() {
            cache.reads.resize(index + 10, false);
        }
        Ok(())
    }

    /// https://dba.stackexchange.com/questions/69471/postgres-update-limit-1/69497#69497
    #[tracing::instrument(skip(self))]
    async fn fetch_unread_messages(&self, limit: usize) -> Result<Vec<TopicMessage>, Self::Error> {
        let cache = self.cache.read().await;
        match cache.is_unread_all_in_memory().await {
            // 全在内存里
            Some(true) => Ok(cache.mem.fetch_unread_messages(limit).await?),
            None => Ok(vec![]),
            Some(false) => {
                let fetch_id_sql = format!(
                    "UPDATE {0}
                    SET loaded = TRUE
                    WHERE message_id = ANY(
                        SELECT message_id
                        FROM {0}
                        WHERE 
                            loaded = FALSE
                            AND read_by IS NULL
                            AND acked = FALSE
                            AND deliver_time IS NULL
                        ORDER BY message_id
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING message_id;",
                    self.table_name
                );
                let fetch_messages_sql = format!(
                    "SELECT
                        message_id,
                        '{}' AS topic_name,
                        producer_name,
                        sequence_id,
                        publish_time,
                        handle_time,
                        deliver_time,
                        payload
                    FROM
                        {}
                    WHERE  message_id = ANY($1);",
                    self.topic_name, self.topic_table_name,
                );
                let messages = self
                    .query({
                        |mut conn| async move {
                            let txn = conn.transaction().await.context(DataBaseSnafu)?;
                            let mut message_ids = Vec::with_capacity(limit);
                            let id_rows = txn
                                .query(&fetch_id_sql, &[&(limit as i64)])
                                .await
                                .context(DataBaseSnafu)?;
                            for row in id_rows {
                                message_ids.push(
                                    row.try_get::<'_, _, i64>("message_id")
                                        .context(DataBaseSnafu)?,
                                );
                            }
                            let mut messages = Vec::with_capacity(message_ids.len());
                            for row in txn
                                .query(&fetch_messages_sql, &[&message_ids])
                                .await
                                .context(DataBaseSnafu)?
                            {
                                let message = TryInto::<PostgresTopicMessage>::try_into(row)
                                    .context(DataBaseSnafu)?;
                                messages.push(message.into());
                            }
                            txn.commit().await.context(DataBaseSnafu)?;
                            Ok(messages)
                        }
                    })
                    .await?;
                Ok(messages)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unacked_messages(
        &self,
        consumer_name: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error> {
        let fetch_sql = format!(
            "SELECT
                message_id,
                '{}' AS topic_name,
                producer_name,
                sequence_id,
                publish_time,
                handle_time,
                deliver_time,
                payload
            FROM
                {}
            WHERE message_id = ANY(
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    read_by = $1 
                    AND acked = FALSE
                    AND deliver_time IS NULL
                LIMIT
                    $2
                OFFSET
                    $3
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );
        self.query(|conn| async move {
            let rows = conn
                .query(
                    &fetch_sql,
                    &[&consumer_name, &(limit as i64), &(from as i64)],
                )
                .await
                .context(DataBaseSnafu)?;
            let mut messages = Vec::with_capacity(limit);
            for row in rows {
                let message =
                    TryInto::<PostgresTopicMessage>::try_into(row).context(DataBaseSnafu)?;
                messages.push(message.into())
            }
            Ok(messages)
        })
        .await
    }

    /// https://dba.stackexchange.com/questions/69471/postgres-update-limit-1/69497#69497
    #[tracing::instrument(skip(self))]
    async fn fetch_unloaded_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error> {
        let fetch_id_sql = format!(
            "UPDATE {0}
            SET loaded = TRUE
            WHERE message_id = ANY(
                SELECT message_id
                FROM {0}
                WHERE 
                    loaded = FALSE
                    AND read_by IS NULL
                    AND acked = FALSE
                    AND deliver_time IS NOT NULL
                    AND deliver_time > $1
                    AND deliver_time <= $2
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            RETURNING message_id;",
            self.table_name
        );
        let fetch_messages_sql = format!(
            "SELECT
                message_id,
                '{}' AS topic_name,
                producer_name,
                sequence_id,
                publish_time,
                handle_time,
                deliver_time,
                payload
            FROM
                {}
            WHERE  message_id = ANY($1);",
            self.topic_name, self.topic_table_name,
        );
        self.query({
            |mut conn| async move {
                let txn = conn.transaction().await.context(DataBaseSnafu)?;
                let mut message_ids = Vec::with_capacity(limit);
                let id_rows = txn
                    .query(&fetch_id_sql, &[&start, &end, &(limit as i64)])
                    .await
                    .context(DataBaseSnafu)?;
                for row in id_rows {
                    message_ids.push(
                        row.try_get::<'_, _, i64>("message_id")
                            .context(DataBaseSnafu)?,
                    );
                }
                let mut messages = Vec::with_capacity(message_ids.len());
                for row in txn
                    .query(&fetch_messages_sql, &[&message_ids])
                    .await
                    .context(DataBaseSnafu)?
                {
                    let message =
                        TryInto::<PostgresTopicMessage>::try_into(row).context(DataBaseSnafu)?;
                    messages.push(message.into());
                }
                txn.commit().await.context(DataBaseSnafu)?;
                Ok(messages)
            }
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unread_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        from: usize,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error> {
        let fetch_sql = format!(
            "SELECT
                message_id,
                '{}' AS topic_name,
                producer_name,
                sequence_id,
                publish_time,
                handle_time,
                deliver_time,
                payload
            FROM
                {}
            WHERE 
                message_id = ANY(
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    loaded = TRUE
                    AND read_by IS NULL 
                    AND acked = FALSE 
                    AND deliver_time IS NOT NULL
                    AND deliver_time > $1
                    AND deliver_time <= $2
                LIMIT
                    $3
                OFFSET
                    $4
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );
        self.query(|conn| async move {
            let rows = conn
                .query(&fetch_sql, &[&start, &end, &(limit as i64), &(from as i64)])
                .await
                .context(DataBaseSnafu)?;
            let mut messages = Vec::with_capacity(limit);
            for row in rows {
                let message =
                    TryInto::<PostgresTopicMessage>::try_into(row).context(DataBaseSnafu)?;
                messages.push(message.into())
            }
            Ok(messages)
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn set_read(&self, consumer_name: &str, message_id: u64) -> Result<(), Self::Error> {
        let update_sql = format!(
            "UPDATE {}
            SET read_by = $1
            WHERE message_id = $2;",
            self.table_name
        );
        self.query({
            let consumer_name = consumer_name.to_owned();
            |conn| async move {
                conn.execute(
                    &update_sql,
                    &[
                        &consumer_name,
                        &(i64::try_from(message_id).context(NumberOverflowSnafu)?),
                    ],
                )
                .await
                .context(DataBaseSnafu)?;
                Ok(())
            }
        })
        .await?;

        let mut cache = self.cache.write().await;
        cache.mem.set_read(consumer_name, message_id).await?;

        let Some(init_id) = cache.init_db_start_unread_id else {
            return Ok(());
        };
        let index = (message_id - init_id) as usize;
        let len = cache.reads.len();
        if index >= len {
            cache.reads.resize(index + 10, false);
        }
        cache.reads.set(index, true);

        // resize
        if cache.reads.len() < MAX_READ_BITS {
            return Ok(());
        }
        match cache.reads.first_zero() {
            Some(first_unread_id) => {
                if first_unread_id == 0 {
                    return Ok(());
                }
                cache.init_db_start_unread_id = Some(init_id + first_unread_id as u64);
                cache.reads = cache.reads.split_off(first_unread_id);
            }
            None => match cache.reads.last_one() {
                Some(_) => {
                    cache.init_db_start_unread_id = None;
                    cache.reads.clear();
                }
                None => unreachable!(),
            },
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn set_acks(&self, message_ids: &[u64]) -> Result<(), Self::Error> {
        let update_sql = format!(
            "UPDATE {}
            SET acked = TRUE
            WHERE message_id = ANY($1);",
            self.table_name
        );
        let mut sql_messages_ids = Vec::with_capacity(message_ids.len());
        for message_id in message_ids {
            let message_id = i64::try_from(*message_id).context(NumberOverflowSnafu)?;
            sql_messages_ids.push(message_id);
        }

        self.query(|conn| async move {
            conn.execute(&update_sql, &[&sql_messages_ids])
                .await
                .context(DataBaseSnafu)?;
            Ok(())
        })
        .await?;

        // 内存
        self.cache.read().await.mem.set_acks(message_ids).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn has_unread(&self) -> Result<bool, Self::Error> {
        Ok(self.cache.read().await.has_unread().await)
    }

    #[tracing::instrument(skip(self))]
    async fn drop(&self) -> Result<(), Self::Error> {
        let drop_sql = format!("DROP TABLE {};", self.table_name);
        self.query(|conn| async move {
            conn.execute(&drop_sql, &[]).await.context(DataBaseSnafu)?;
            Ok(())
        })
        .await?;
        // 内存
        self.cache.read().await.mem.drop().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn retain_acked(
        &self,
        num_limit: Option<usize>,
        create_after: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Error> {
        let mut max_message_id = None::<i64>;
        if let Some(num_limit) = num_limit {
            let fetch_sql = format!(
                "SELECT 
                    message_id
                FROM 
                    {}
                WHERE 
                    acked = TRUE
                ORDER BY 
                    message_id DESC
                LIMIT 1 OFFSET $1;",
                self.table_name
            );
            max_message_id = self
                .query(|conn| async move {
                    let message_id = conn
                        .query_opt(&fetch_sql, &[&(num_limit as i64)])
                        .await
                        .context(DataBaseSnafu)?
                        .map(|row| row.try_get::<'_, _, i64>("message_id"))
                        .transpose()
                        .context(DataBaseSnafu)?;
                    Ok(message_id)
                })
                .await?;
        }

        if let Some(create_after) = create_after {
            let fetch_sql = format!(
                "SELECT
                    message_id
                FROM 
                    {} 
                WHERE
                    create_time > $1
                ORDER BY 
                    message_id
                LIMIT 1;",
                self.table_name
            );
            let time_message_id: Option<i64> = self
                .query(|conn| async move {
                    let message_id = conn
                        .query_opt(&fetch_sql, &[&create_after])
                        .await
                        .context(DataBaseSnafu)?
                        .map(|row| row.try_get::<'_, _, i64>("message_id"))
                        .transpose()
                        .context(DataBaseSnafu)?;
                    Ok(message_id)
                })
                .await?;
            max_message_id = cmp::max(max_message_id, time_message_id);
        }

        let Some(max_message_id) = max_message_id else {
            return Ok(());
        };

        let delete_sql = format!(
            "DELETE FROM {} WHERE acked = TRUE AND message_id < $1;",
            self.table_name
        );
        self.query(|conn| async move {
            conn.execute(&delete_sql, &[&max_message_id])
                .await
                .context(DataBaseSnafu)
        })
        .await?;

        self.cache
            .read()
            .await
            .mem
            .retain_acked(num_limit, create_after)
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn drain_acked(&self) -> Result<(), Self::Error> {
        let delete_sql = format!("DELETE FROM {} WHERE acked = TRUE;", self.table_name);
        self.query(
            |conn| async move { conn.execute(&delete_sql, &[]).await.context(DataBaseSnafu) },
        )
        .await?;

        self.cache.read().await.mem.drain_acked().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn first_unacked_id(&self) -> Result<Option<u64>, Self::Error> {
        let fetch_sql = format!(
            "SELECT
                message_id
            FROM
                {}
            WHERE
                acked = FALSE
            ORDER BY
                message_id
            LIMIT 1;",
            self.table_name
        );
        let message_id: Option<i64> = self
            .query(|conn| async move {
                conn.query_opt(&fetch_sql, &[])
                    .await
                    .context(DataBaseSnafu)?
                    .map(|row| row.try_get::<'_, _, i64>("message_id"))
                    .transpose()
                    .context(DataBaseSnafu)
            })
            .await?;
        Ok(message_id.map(|n| n as u64))
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_loaded_unread_messages(&self) -> Result<Vec<TopicMessage>, Self::Error> {
        let fetch_sql = format!(
            "SELECT
                message_id,
                '{}' AS topic_name,
                producer_name,
                sequence_id,
                publish_time,
                handle_time,
                deliver_time,
                payload
            FROM
                {}
            WHERE message_id = ANY(
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    loaded = TRUE
                    AND read_by IS NULL
                    AND acked = FALSE
                    AND deliver_time IS NULL
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );
        let messages = self
            .query({
                |conn| async move {
                    let mut messages = Vec::new();
                    for row in conn.query(&fetch_sql, &[]).await.context(DataBaseSnafu)? {
                        let message = TryInto::<PostgresTopicMessage>::try_into(row)
                            .context(DataBaseSnafu)?;
                        messages.push(message.into());
                    }

                    Ok(messages)
                }
            })
            .await?;
        Ok(messages)
    }
}

async fn query<F, Fut, R>(pool: &deadpool_postgres::Pool, f: F) -> Result<R, Error>
where
    F: FnOnce(deadpool_postgres::Object) -> Fut,
    Fut: Future<Output = Result<R, Error>>,
{
    let client = pool.get().await.context(PoolSnafu)?;
    f(client).await
}
