use std::{cmp, path::PathBuf, sync::Arc};

use bitvec::{order::Lsb0, vec::BitVec};
use chrono::{DateTime, Utc};
use comet_common::{
    error::ResponsiveError,
    protocol::response::{Response, ReturnCode},
    types::InitialPosition,
};

use rusqlite::{OptionalExtension, Row, TransactionBehavior};
use snafu::{Location, ResultExt, Snafu};
use tokio::sync::{mpsc, RwLock};

use super::{
    mem::{MemorySubscriptionStorage, MemoryTopicStorage},
    PublishedMessage, SubscriptionStorage, TopicMessage, TopicStorage,
};

const MAX_READ_BITS: usize = 8 * 1024 * 1024 * 100;
const INIT_LOAD_MESSAGES_NUM: usize = 1000;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("build sqlite pool error"))]
    SqlitePoolBuild {
        #[snafu(source)]
        error: deadpool_sqlite::BuildError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("get sqlite conn error"))]
    SqlitePool {
        #[snafu(source)]
        error: deadpool_sqlite::PoolError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("sqlite interact error"))]
    SqliteInteract {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("sqlite database error"))]
    DataBase {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("memory error"), context(false))]
    Memory {
        source: super::mem::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("create sqlite data dir {} error", path.display()))]
    MakeDataDir {
        path: PathBuf,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> Response {
        Response::new(ReturnCode::Internal, self.to_string())
    }
}

pub struct SqliteTopicMessage {
    pub message_id: u64,
    pub topic_name: String,
    pub producer_name: String,
    pub sequence_id: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub payload: Vec<u8>,
}

impl TryFrom<&Row<'_>> for SqliteTopicMessage {
    type Error = rusqlite::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: row.get("message_id")?,
            topic_name: row.get("topic_name")?,
            producer_name: row.get("producer_name")?,
            sequence_id: row.get("sequence_id")?,
            publish_time: row.get("publish_time")?,
            deliver_time: row.get("deliver_time")?,
            handle_time: row.get("handle_time")?,
            payload: row.get("payload")?,
        })
    }
}

impl From<SqliteTopicMessage> for TopicMessage {
    fn from(message: SqliteTopicMessage) -> Self {
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
    pub(crate) data_dir: PathBuf,
    pub(crate) mem_messages_num_limit: Option<usize>,
}

#[derive(Clone)]
pub struct SqliteTopicStorage {
    topic_name: String,
    table_name: String,
    config: Config,
    pool: deadpool_sqlite::Pool,
    mem: MemoryTopicStorage,
}

impl SqliteTopicStorage {
    #[tracing::instrument]
    pub async fn new(topic_name: &str, config: Config) -> Result<Self, Error> {
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .context(MakeDataDirSnafu {
                path: &config.data_dir,
            })?;
        // 创建连接
        let pool = deadpool_sqlite::Config::new(
            config
                .data_dir
                .join(format!("comet_storage_{topic_name}.sqlite")),
        )
        .builder(deadpool_sqlite::Runtime::Tokio1)
        .expect("infallible")
        .build()
        .context(SqlitePoolBuildSnafu)?;
        let mem = MemoryTopicStorage::new();

        // 创建 topic 表
        let table_name = format!("comet_topic_{topic_name}");
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (\n
                message_id      INTEGER PRIMARY KEY AUTOINCREMENT,\n
                producer_name   TEXT    NOT NULL,\n
                sequence_id     INTEGER NOT NULL,\n
                publish_time    INTEGER NOT NULL,\n
                handle_time     INTEGER NOT NULL,\n
                deliver_time    INTEGER,\n
                payload         BLOB    NOT NULL,\n
                create_time     INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,\n
                UNIQUE (producer_name, sequence_id)
            );",
        );
        query(&pool, move |conn| {
            conn.execute(&create_table_sql, ()).context(DataBaseSnafu)
        })
        .await??;

        // 向内存中加载一些数据
        let fetch_sql = format!(
            "SELECT
                message_id,
                '{topic_name}' AS topic_name,
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
            INIT_LOAD_MESSAGES_NUM
        );

        let (init_tx, mut init_rx) = mpsc::channel::<SqliteTopicMessage>(50);
        let handle = tokio::spawn({
            let mem = mem.clone();
            async move {
                while let Some(message) = init_rx.recv().await {
                    mem.push_message(message.into()).await;
                }
            }
        });
        query(&pool, move |conn| {
            let mut stmt = conn.prepare(&fetch_sql).context(DataBaseSnafu)?;
            let mut rows = stmt
                .query_map((), |row| TryInto::<SqliteTopicMessage>::try_into(row))
                .context(DataBaseSnafu)?;

            while let Some(message) = rows.next().transpose().context(DataBaseSnafu)? {
                if init_tx.blocking_send(message).is_err() {
                    break;
                }
            }

            Ok::<_, Error>(())
        })
        .await??;
        handle.await.expect("init topic messages panic");

        Ok(Self {
            config,
            pool,
            topic_name: topic_name.to_owned(),
            table_name,
            mem,
        })
    }

    async fn query<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        query(&self.pool, f).await
    }
}

impl TopicStorage for SqliteTopicStorage {
    type Error = Error;

    type SubscriptionStorage = SqliteSubscriptionStorage;

    #[tracing::instrument(skip(self))]
    async fn create_subscription(
        &self,
        subscription_name: &str,
        initial_position: InitialPosition,
    ) -> Result<Self::SubscriptionStorage, Self::Error> {
        SqliteSubscriptionStorage::new(
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
                ?1, ?2, ?3, ?4, ?5, ?6
            ) RETURNING message_id;",
            self.table_name,
        );

        let message_id = self
            .query({
                let message = message.clone();
                move |conn| {
                    conn.query_row(
                        &insert_sql,
                        (
                            message.producer_name,
                            message.sequence_id,
                            message.publish_time,
                            message.handle_time,
                            message.deliver_time,
                            message.payload.to_vec(),
                        ),
                        |row| row.get("message_id"),
                    )
                    .context(DataBaseSnafu)
                }
            })
            .await??;

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
                let delete_sql = format!("DELETE FROM {} WHERE message_id < ?1;", self.table_name);
                self.query(move |conn| {
                    conn.execute(&delete_sql, [min_id]).context(DataBaseSnafu)?;
                    Ok::<_, Self::Error>(())
                })
                .await??;
            }
            None => {
                let delete_sql = format!("DELETE FROM {};", self.table_name);
                self.query(move |conn| {
                    conn.execute(&delete_sql, ()).context(DataBaseSnafu)?;
                    Ok::<_, Self::Error>(())
                })
                .await??;
            }
        }

        // 删除内存
        self.mem.retain_messages(min_message_id).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn drop(&self) -> Result<(), Self::Error> {
        let drop_sql = format!("DROP TABLE {}", self.table_name);
        self.query(move |conn| {
            conn.execute(&drop_sql, ()).context(DataBaseSnafu)?;
            Ok::<_, Self::Error>(())
        })
        .await??;
        // 内存
        self.mem.drop().await?;
        Ok(())
    }

    async fn last_sequence_id(&self, producer_name: &str) -> Result<u64, Self::Error> {
        let fetch_sql = format!(
            "SELECT sequence_id
            FROM {}
            WHERE producer_name = ?1
            ORDER BY sequence_id DESC
            LIMIT 1;",
            self.table_name
        );
        let sequence_id = self
            .query({
                let producer_name = producer_name.to_owned();
                move |conn| {
                    conn.query_row(&fetch_sql, (producer_name,), |row| row.get("sequence_id"))
                        .optional()
                        .context(DataBaseSnafu)
                }
            })
            .await??;
        Ok(sequence_id.unwrap_or_default())
    }
}

#[derive(Clone)]
pub struct SqliteSubscriptionStorage {
    topic_name: String,
    topic_table_name: String,
    table_name: String,
    pool: deadpool_sqlite::Pool,
    cache: Arc<RwLock<super::cache::SubscriptionStorageCache>>,
}

impl SqliteSubscriptionStorage {
    #[tracing::instrument(skip(pool, topic_storage))]
    async fn new(
        topic_name: String,
        topic_table_name: String,
        subscription_name: String,
        initial_position: InitialPosition,
        pool: deadpool_sqlite::Pool,
        topic_storage: MemoryTopicStorage,
    ) -> Result<Self, Error> {
        let table_name = format!("comet_sub_{}_{}", topic_name, subscription_name);

        query(&pool, {
            let table_name = table_name.clone();
            let topic_table_name = topic_table_name.clone();
            move |conn| {
                // 开启事务
                let txn = conn
                    .transaction_with_behavior(TransactionBehavior::Exclusive)
                    .context(DataBaseSnafu)?;

                // 判断表是否存在
                let check_sql = format!(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
                );
                if txn
                    .query_row(&check_sql, (), |row| row.get::<_, String>(0))
                    .optional()
                    .context(DataBaseSnafu)?
                    .is_some()
                {
                    txn.commit().context(DataBaseSnafu)?;
                    return Ok::<_, Error>(());
                }

                // 创建 subscription 表
                let create_table_sql = format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (\n
                        message_id      INTEGER PRIMARY KEY,\n
                        loaded          BOOLEAN DEFAULT 0 CHECK (loaded IN (0, 1)),\n
                        read_by         TEXT    DEFAULT NULL,\n
                        acked           BOOLEAN DEFAULT 0 CHECK (loaded IN (0, 1)),\n
                        create_time     INTEGER NOT NULL,\n
                        deliver_time    INTEGER\n
                    );",
                );
                txn.execute(&create_table_sql, ()).context(DataBaseSnafu)?;

                // 生成记录
                if matches!(initial_position, InitialPosition::Earliest) {
                    let insert_sql = format!(
                        "INSERT INTO {table_name} \
                        (message_id, create_time, deliver_time) \
                        SELECT message_id, create_time, deliver_time \
                        FROM {}",
                        topic_table_name,
                    );
                    txn.execute(&insert_sql, ()).context(DataBaseSnafu)?;
                }

                // 创建索引
                let create_time_idx_sql =
                    format!("CREATE INDEX idx_create_time ON {table_name} (create_time);");
                let deliver_time_idx_sql =
                    format!("CREATE INDEX idx_deliver_time ON {table_name} (deliver_time);");
                // 创建索引
                txn.execute_batch(&format!(
                    "{create_time_idx_sql}\n
                    {deliver_time_idx_sql}\n",
                ))
                .context(DataBaseSnafu)?;

                txn.commit().context(DataBaseSnafu)?;
                Ok(())
            }
        })
        .await??;

        let init_db_start_id = {
            let fetch_sql = format!(
                "SELECT message_id FROM {table_name} WHERE read_by IS NULL ORDER BY message_id LIMIT 1;",
            );
            query(&pool, move |conn| {
                conn.query_row(&fetch_sql, (), |row| row.get::<_, u64>("message_id"))
                    .optional()
                    .context(DataBaseSnafu)
            })
            .await??
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
                        AND message_id >= ?1
                    ;",
                );
                query(&pool, move |conn| {
                    let mut reads = BitVec::<u64, Lsb0>::new();
                    let mut stmt = conn.prepare(&fetch_sql).context(DataBaseSnafu)?;
                    let mut rows = stmt
                        .query_map((init_start_id,), |row| row.get::<_, u64>("message_id"))
                        .context(DataBaseSnafu)?;
                    while let Some(message_id) = rows.next().transpose().context(DataBaseSnafu)? {
                        let index = (message_id - init_start_id) as usize;
                        let len = reads.len();
                        if index >= reads.capacity() {
                            reads.resize(cmp::max(len + 10, index + 10), false);
                        }
                        reads.set(index, true);
                    }
                    Ok::<_, Error>(reads)
                })
                .await??
            }
            None => BitVec::<u64, Lsb0>::new(),
        };

        Ok(Self {
            topic_table_name,
            pool,
            topic_name,
            table_name,
            cache: Arc::new(RwLock::new(super::cache::SubscriptionStorageCache {
                init_db_start_unread_id: init_db_start_id,
                mem: MemorySubscriptionStorage::new(topic_storage),
                reads,
            })),
        })
    }

    async fn query<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        query(&self.pool, f).await
    }
}

impl SubscriptionStorage for SqliteSubscriptionStorage {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    async fn add_record(&self, message_id: u64) -> Result<(), Self::Error> {
        let insert_sql = format!(
            "INSERT INTO {} (
                message_id,
                create_time
            ) SELECT 
                message_id,
                create_time
            FROM
                {}
            WHERE
                message_id = ?1;",
            self.table_name, self.topic_table_name
        );
        self.query(move |conn| {
            conn.execute(&insert_sql, (message_id,))
                .context(DataBaseSnafu)?;
            Ok::<_, Self::Error>(())
        })
        .await??;
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
            WHERE 
                message_id 
            IN (
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    loaded = TRUE
                    AND acked = FALSE
                    AND read_by IS NULL
                    AND deliver_time IS NULL
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );

        self.query({
            move |conn| {
                let mut stmt = conn.prepare(&fetch_sql).context(DataBaseSnafu)?;
                let rows = stmt
                    .query_map((), |row| TryInto::<SqliteTopicMessage>::try_into(row))
                    .context(DataBaseSnafu)?;
                let mut messages = Vec::new();
                for row in rows {
                    messages.push(row.context(DataBaseSnafu)?.into())
                }
                Ok(messages)
            }
        })
        .await?
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unread_messages(&self, limit: usize) -> Result<Vec<TopicMessage>, Self::Error> {
        let cache = self.cache.read().await;
        match cache.is_unread_all_in_memory().await {
            // 全在内存里
            Some(true) => Ok(cache.mem.fetch_unread_messages(limit).await?),
            None => Ok(vec![]),
            Some(false) => {
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
                        message_id 
                    IN (
                        SELECT
                            message_id
                        FROM
                            {}
                        WHERE 
                            loaded = FALSE
                            AND acked = FALSE
                            AND read_by IS NULL
                            AND deliver_time IS NULL
                        LIMIT
                            ?1
                    );",
                    self.topic_name, self.topic_table_name, self.table_name
                );
                let update_sql = format!(
                    "UPDATE  
                        {0}
                    SET 
                        loaded = TRUE
                    WHERE 
                        message_id 
                    IN (
                        SELECT
                            message_id
                        FROM
                            {0}
                        WHERE 
                            loaded = FALSE
                            AND acked = FALSE
                            AND read_by IS NULL
                            AND deliver_time IS NULL
                        LIMIT
                            ?1
                    );",
                    self.table_name
                );
                let messages = self
                    .query({
                        move |conn| {
                            let mut messages = Vec::with_capacity(limit);

                            let txn = conn
                                .transaction_with_behavior(TransactionBehavior::Exclusive)
                                .context(DataBaseSnafu)?;
                            {
                                let mut stmt = txn.prepare(&fetch_sql).context(DataBaseSnafu)?;
                                let rows = stmt
                                    .query_map((limit,), |row| {
                                        TryInto::<SqliteTopicMessage>::try_into(row)
                                    })
                                    .context(DataBaseSnafu)?;
                                for row in rows {
                                    messages.push(row.context(DataBaseSnafu)?.into())
                                }
                            }
                            txn.execute(&update_sql, (limit,)).context(DataBaseSnafu)?;
                            txn.commit().context(DataBaseSnafu)?;
                            Ok::<_, Self::Error>(messages)
                        }
                    })
                    .await??;
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
            WHERE 
                message_id 
            IN (
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    read_by = ?1 
                    AND acked = FALSE
                    AND deliver_time IS NULL
                LIMIT
                    ?2
                OFFSET
                    ?3
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );
        self.query({
            let consumer_name = consumer_name.to_owned();
            move |conn| {
                let mut stmt = conn.prepare(&fetch_sql).context(DataBaseSnafu)?;
                let rows = stmt
                    .query_map((consumer_name, limit, from), |row| {
                        TryInto::<SqliteTopicMessage>::try_into(row)
                    })
                    .context(DataBaseSnafu)?;
                let mut messages = Vec::with_capacity(limit);
                for row in rows {
                    messages.push(row.context(DataBaseSnafu)?.into())
                }
                Ok(messages)
            }
        })
        .await?
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unloaded_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
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
                message_id 
            IN (
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    loaded = FALSE 
                    AND acked = FALSE
                    AND deliver_time IS NOT NULL 
                    AND deliver_time > ?1
                    AND deliver_time <= ?2
                LIMIT
                    ?3
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );
        let update_sql = format!(
            "UPDATE {0}
            SET
                loaded = TRUE
            WHERE 
                message_id 
            IN (
                SELECT
                    message_id
                FROM
                    {0}
                WHERE 
                    loaded = FALSE 
                    AND acked = FALSE
                    AND deliver_time IS NOT NULL 
                    AND deliver_time > ?1
                    AND deliver_time <= ?2
                LIMIT
                    ?3
            );",
            self.table_name
        );
        self.query({
            move |conn| {
                let mut messages = Vec::with_capacity(limit);
                // 开启事务
                let txn = conn
                    .transaction_with_behavior(TransactionBehavior::Exclusive)
                    .context(DataBaseSnafu)?;
                // 读取
                {
                    let mut stmt = txn.prepare(&fetch_sql).context(DataBaseSnafu)?;
                    let rows = stmt
                        .query_map((start, end, limit), |row| {
                            TryInto::<SqliteTopicMessage>::try_into(row)
                        })
                        .context(DataBaseSnafu)?;
                    for row in rows {
                        messages.push(row.context(DataBaseSnafu)?.into())
                    }
                }
                txn.execute(&update_sql, (start, end, limit))
                    .context(DataBaseSnafu)?;
                txn.commit().context(DataBaseSnafu)?;
                Ok(messages)
            }
        })
        .await?
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
                message_id 
            IN (
                SELECT
                    message_id
                FROM
                    {}
                WHERE 
                    acked = FALSE
                    AND read_by IS NULL
                    AND deliver_time IS NOT NULL 
                    AND deliver_time > ?1
                    AND deliver_time <= ?2
                LIMIT
                    ?3
                OFFSET
                    ?4
            );",
            self.topic_name, self.topic_table_name, self.table_name
        );
        self.query(move |conn| {
            let mut stmt = conn.prepare(&fetch_sql).context(DataBaseSnafu)?;
            let rows = stmt
                .query_map((start, end, limit, from), |row| {
                    TryInto::<SqliteTopicMessage>::try_into(row)
                })
                .context(DataBaseSnafu)?;
            let mut messages = Vec::with_capacity(limit);
            for row in rows {
                messages.push(row.context(DataBaseSnafu)?.into())
            }
            Ok(messages)
        })
        .await?
    }

    #[tracing::instrument(skip(self))]
    async fn set_read(&self, consumer_name: &str, message_id: u64) -> Result<(), Self::Error> {
        let update_sql = format!(
            "UPDATE {}
            SET read_by = ?1
            WHERE message_id = ?2;",
            self.table_name
        );
        self.query({
            let consumer_name = consumer_name.to_owned();
            move |conn| {
                conn.execute(&update_sql, (consumer_name, message_id))
                    .context(DataBaseSnafu)?;
                Ok::<_, Self::Error>(())
            }
        })
        .await??;

        let mut cache = self.cache.write().await;
        cache.mem.set_read(consumer_name, message_id).await?;

        let Some(init_id) = cache.init_db_start_unread_id else {
            return Ok(());
        };
        let index = (message_id - init_id) as usize;
        let len = cache.reads.len();
        if index >= cache.reads.capacity() {
            cache.reads.resize(cmp::max(len + 10, index + 10), false);
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
        let mut batch_sql = String::new();
        for message_ids in message_ids.chunks(100) {
            let message_ids = message_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join(",");
            let update_sql = format!(
                "UPDATE {}
                SET acked = TRUE
                WHERE message_id IN ({});",
                self.table_name, message_ids
            );
            batch_sql.push_str(&update_sql);
        }

        self.query(move |conn| {
            conn.execute_batch(&batch_sql).context(DataBaseSnafu)?;
            Ok::<_, Self::Error>(())
        })
        .await??;

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
        let drop_sql = format!("DROP TABLE {}", self.table_name);
        self.query(move |conn| {
            conn.execute(&drop_sql, ()).context(DataBaseSnafu)?;
            Ok::<_, Self::Error>(())
        })
        .await??;
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
                    acked = 1
                ORDER BY 
                    message_id DESC
                LIMIT 1 OFFSET ?1;",
                self.table_name
            );
            max_message_id = self
                .query(move |conn| {
                    conn.query_row(&fetch_sql, (num_limit,), |row| row.get("message_id"))
                        .optional()
                        .context(DataBaseSnafu)
                })
                .await??;
        }

        if let Some(create_after) = create_after {
            let fetch_sql = format!(
                "SELECT
                    message_id
                FROM 
                    {} 
                WHERE
                    create_time > ?1
                ORDER BY 
                    message_id
                LIMIT 1;",
                self.table_name
            );
            let time_message_id: Option<i64> = self
                .query(move |conn| {
                    conn.query_row(&fetch_sql, (create_after,), |row| row.get("message_id"))
                        .optional()
                        .context(DataBaseSnafu)
                })
                .await??;
            max_message_id = cmp::max(max_message_id, time_message_id);
        }

        let Some(max_message_id) = max_message_id else {
            return Ok(());
        };

        let delete_sql = format!(
            "DELETE FROM {} WHERE acked = 1 AND message_id < ?1",
            self.table_name
        );
        self.query(move |conn| {
            conn.execute(&delete_sql, (max_message_id,))
                .context(DataBaseSnafu)
        })
        .await??;

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
        let delete_sql = format!("DELETE FROM {} WHERE acked = 1", self.table_name);
        self.query(move |conn| conn.execute(&delete_sql, ()).context(DataBaseSnafu))
            .await??;

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
        let message_id = self
            .query(move |conn| {
                conn.query_row(&fetch_sql, (), |row| row.get("message_id"))
                    .optional()
                    .context(DataBaseSnafu)
            })
            .await??;
        Ok(message_id)
    }
}

async fn query<F, R>(pool: &deadpool_sqlite::Pool, f: F) -> Result<R, Error>
where
    F: FnOnce(&mut rusqlite::Connection) -> R + Send + 'static,
    R: Send + 'static,
{
    pool.get()
        .await
        .context(SqlitePoolSnafu)?
        .interact(f)
        .await
        .map_err(|_| SqliteInteractSnafu.build())
}
