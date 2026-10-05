use std::{future::Future, pin::Pin, time::Duration};

use metrics::{counter, gauge};
use rdkafka::{
    ClientConfig,
    error::KafkaError,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::HealthState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaDispatcherConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub queue_buffering_max_messages: usize,
    pub delivery_timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaOutboxMessage {
    pub schema_version: u32,
    pub event_id: i64,
    pub chain_id: String,
    pub transition_kind: String,
    pub block_hash: String,
    pub block_height: u64,
    pub ordering: KafkaOutboxOrdering,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaOutboxOrdering {
    pub event_id: i64,
    pub block_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaDispatchOutcome {
    Delivered(KafkaOutboxMessage),
    Idle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaDispatchFailurePoint {
    BeforePublish,
    AfterBrokerAckBeforeMark,
}

#[derive(Debug)]
pub struct KafkaOutboxDispatcher<P = RdkafkaOutboxProducer> {
    pool: PgPool,
    topic: String,
    producer: P,
    poll_interval: Duration,
    health: Option<HealthState>,
}

#[derive(Clone)]
pub struct RdkafkaOutboxProducer {
    inner: FutureProducer,
    queue_timeout: Duration,
}

#[derive(Debug, Error)]
pub enum KafkaDispatchError {
    #[error("kafka brokers are required before starting the outbox dispatcher")]
    MissingBrokers,
    #[error("kafka topic is required before starting the outbox dispatcher")]
    MissingTopic,
    #[error("kafka producer configuration failed: {0}")]
    ProducerConfig(#[from] KafkaError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("outbox row {event_id} contains invalid block hash length {length}")]
    InvalidBlockHash { event_id: i64, length: usize },
    #[error("outbox row {event_id} contains a negative block height {height}")]
    InvalidBlockHeight { event_id: i64, height: i64 },
    #[error("failed to serialize outbox event {event_id}: {source}")]
    Serialize {
        event_id: i64,
        source: serde_json::Error,
    },
    #[error("kafka publish failed for event {event_id}: {message}")]
    Publish { event_id: i64, message: String },
    #[error("injected dispatcher failure before publishing event {event_id}")]
    InjectedBeforePublish { event_id: i64 },
    #[error("injected dispatcher failure after broker acknowledgement for event {event_id}")]
    InjectedAfterAck { event_id: i64 },
}

#[derive(Debug)]
struct OutboxRow {
    event_id: i64,
    chain_id: String,
    event_kind: String,
    block_hash: Vec<u8>,
    block_height: i64,
    payload: Value,
}

pub trait OutboxProducer: Send + Sync {
    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: &'a KafkaOutboxMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

impl KafkaDispatcherConfig {
    #[must_use]
    pub fn new(
        brokers: Vec<String>,
        topic: String,
        queue_buffering_max_messages: usize,
        delivery_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            brokers,
            topic,
            queue_buffering_max_messages,
            delivery_timeout,
            poll_interval,
        }
    }
}

impl KafkaOutboxDispatcher<RdkafkaOutboxProducer> {
    /// Builds a dispatcher backed by an `rdkafka` durable producer.
    ///
    /// # Errors
    ///
    /// Returns an error when required Kafka settings are missing or librdkafka rejects the
    /// producer configuration.
    pub fn connect(
        pool: PgPool,
        config: KafkaDispatcherConfig,
    ) -> Result<Self, KafkaDispatchError> {
        if config.brokers.is_empty() || config.brokers.iter().any(|broker| broker.trim().is_empty())
        {
            return Err(KafkaDispatchError::MissingBrokers);
        }
        if config.topic.trim().is_empty() {
            return Err(KafkaDispatchError::MissingTopic);
        }
        let producer = RdkafkaOutboxProducer::connect(&config)?;
        Ok(Self::new(
            pool,
            config.topic,
            producer,
            config.poll_interval,
        ))
    }

    /// Opens a Postgres pool and builds a Kafka outbox dispatcher.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres cannot be reached or Kafka producer configuration fails.
    pub async fn connect_database(
        database_url: &str,
        config: KafkaDispatcherConfig,
    ) -> Result<Self, KafkaDispatchError> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(database_url)
            .await?;
        Self::connect(pool, config)
    }
}

impl<P> KafkaOutboxDispatcher<P>
where
    P: OutboxProducer,
{
    #[must_use]
    pub fn new(pool: PgPool, topic: String, producer: P, poll_interval: Duration) -> Self {
        Self {
            pool,
            topic,
            producer,
            poll_interval,
            health: None,
        }
    }

    #[must_use]
    pub fn with_health(mut self, health: HealthState) -> Self {
        self.health = Some(health);
        self
    }

    /// Dispatches unpublished rows until cancellation or a publish error.
    ///
    /// Rows are dispatched one at a time in `event_id` order. A publish error stops the loop so
    /// operators can inspect the first blocked row without event N+1 being published first.
    ///
    /// # Errors
    ///
    /// Returns database, serialization, and Kafka publish errors from [`Self::dispatch_once`].
    pub async fn run_until_cancelled(
        &self,
        cancel: CancellationToken,
    ) -> Result<(), KafkaDispatchError> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            match self.dispatch_once().await? {
                KafkaDispatchOutcome::Delivered(message) => {
                    info!(
                        event_id = message.event_id,
                        transition_kind = message.transition_kind,
                        block_height = message.block_height,
                        "published outbox event to Kafka"
                    );
                }
                KafkaDispatchOutcome::Idle => sleep(self.poll_interval).await,
            }
        }
    }

    /// Dispatches at most one unpublished outbox row.
    ///
    /// # Errors
    ///
    /// Returns an error if the row cannot be serialized, published, or marked after acknowledgement.
    pub async fn dispatch_once(&self) -> Result<KafkaDispatchOutcome, KafkaDispatchError> {
        self.dispatch_once_inner(None).await
    }

    /// Dispatches one row with a deterministic failure point for acceptance tests.
    ///
    /// # Errors
    ///
    /// Returns the injected failure or the same errors as [`Self::dispatch_once`].
    pub async fn dispatch_once_with_failure(
        &self,
        failure: KafkaDispatchFailurePoint,
    ) -> Result<KafkaDispatchOutcome, KafkaDispatchError> {
        self.dispatch_once_inner(Some(failure)).await
    }

    async fn dispatch_once_inner(
        &self,
        failure: Option<KafkaDispatchFailurePoint>,
    ) -> Result<KafkaDispatchOutcome, KafkaDispatchError> {
        let mut tx = self.pool.begin().await?;
        let Some(row) = next_unpublished_row(&mut tx).await? else {
            tx.commit().await?;
            self.refresh_outbox_metrics_best_effort().await;
            return Ok(KafkaDispatchOutcome::Idle);
        };
        let message = row_to_message(row)?;

        if failure == Some(KafkaDispatchFailurePoint::BeforePublish) {
            return Err(KafkaDispatchError::InjectedBeforePublish {
                event_id: message.event_id,
            });
        }

        if let Err(error_message) = self.producer.publish(&self.topic, &message).await {
            counter!("chainweave_kafka_delivery_failures_total").increment(1);
            if let Some(health) = &self.health {
                health.mark_degraded("kafka").await;
            }
            return Err(KafkaDispatchError::Publish {
                event_id: message.event_id,
                message: error_message,
            });
        }

        if failure == Some(KafkaDispatchFailurePoint::AfterBrokerAckBeforeMark) {
            return Err(KafkaDispatchError::InjectedAfterAck {
                event_id: message.event_id,
            });
        }

        let updated = sqlx::query!(
            r"
            UPDATE outbox_events
            SET published_at = now()
            WHERE event_id = $1 AND published_at IS NULL
            ",
            message.event_id,
        )
        .execute(tx.as_mut())
        .await?;
        if updated.rows_affected() == 0 {
            warn!(
                event_id = message.event_id,
                "outbox event was already marked published"
            );
        }
        tx.commit().await?;
        info!(
            chain_id = %message.chain_id,
            block_hash = %message.block_hash,
            block_height = message.block_height,
            event_id = message.event_id,
            transition_kind = %message.transition_kind,
            "marked outbox event delivered"
        );
        self.refresh_outbox_metrics_best_effort().await;
        Ok(KafkaDispatchOutcome::Delivered(message))
    }

    async fn refresh_outbox_metrics_best_effort(&self) {
        if let Err(error) = refresh_outbox_metrics(&self.pool).await {
            warn!(error = %error, "failed to refresh Kafka outbox metrics");
        }
    }
}

impl RdkafkaOutboxProducer {
    /// Creates a durable Kafka producer for at-least-once outbox delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when librdkafka rejects the configuration.
    pub fn connect(config: &KafkaDispatcherConfig) -> Result<Self, KafkaDispatchError> {
        let brokers = config.brokers.join(",");
        let inner = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set(
                "queue.buffering.max.messages",
                config.queue_buffering_max_messages.to_string(),
            )
            .set(
                "delivery.timeout.ms",
                config.delivery_timeout.as_millis().to_string(),
            )
            .set(
                "message.timeout.ms",
                config.delivery_timeout.as_millis().to_string(),
            )
            .set("max.in.flight.requests.per.connection", "1")
            .create::<FutureProducer>()?;
        Ok(Self {
            inner,
            queue_timeout: config.delivery_timeout,
        })
    }
}

impl OutboxProducer for RdkafkaOutboxProducer {
    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: &'a KafkaOutboxMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(message).map_err(|error| error.to_string())?;
            // Block-level transitions use a chain-scoped key to preserve source order within the
            // chosen partition. Stable event_id is carried in headers and payload for deduplication.
            let key = format!("chain:{}", message.chain_id);
            let event_id = message.event_id.to_string();
            let schema_version = message.schema_version.to_string();
            let record = FutureRecord::to(topic).key(&key).payload(&payload).headers(
                OwnedHeaders::new()
                    .insert(Header {
                        key: "event_id",
                        value: Some(event_id.as_bytes()),
                    })
                    .insert(Header {
                        key: "schema_version",
                        value: Some(schema_version.as_bytes()),
                    }),
            );

            self.inner
                .send(record, self.queue_timeout)
                .await
                .map(|_| ())
                .map_err(|(error, _message)| error.to_string())
        })
    }
}

pub fn render_demo_consumer_event(value: &Value) -> String {
    let event_id = value
        .get("event_id")
        .and_then(Value::as_i64)
        .map_or_else(|| "unknown".to_owned(), |event_id| event_id.to_string());
    let transition = value
        .get("transition_kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let block_height = value
        .get("block_height")
        .and_then(Value::as_u64)
        .map_or_else(|| "unknown".to_owned(), |height| height.to_string());
    let block_hash = value
        .get("block_hash")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    if transition == "rollback" {
        return format!(
            "rollback event_id={event_id} block_height={block_height} block_hash={block_hash}"
        );
    }

    let decoded = value
        .pointer("/payload/logs")
        .and_then(Value::as_array)
        .map(|logs| {
            logs.iter()
                .filter_map(|log| log.get("decoded_event"))
                .filter(|decoded| !decoded.is_null())
                .count()
        })
        .unwrap_or(0);
    format!(
        "{transition} event_id={event_id} block_height={block_height} block_hash={block_hash} decoded_events={decoded}"
    )
}

async fn next_unpublished_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<OutboxRow>, sqlx::Error> {
    sqlx::query_as!(
        OutboxRow,
        r#"
        SELECT
            event_id,
            chain_id::text AS "chain_id!",
            event_kind,
            block_hash,
            block_height,
            payload
        FROM outbox_events
        WHERE published_at IS NULL
        ORDER BY event_id
        LIMIT 1
        FOR UPDATE
        "#,
    )
    .fetch_optional(tx.as_mut())
    .await
}

async fn refresh_outbox_metrics(pool: &PgPool) -> Result<(), sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT
            COUNT(*)::BIGINT AS "unpublished_count!",
            COALESCE(
                EXTRACT(EPOCH FROM (now() - MIN(created_at))),
                0
            )::DOUBLE PRECISION AS "oldest_age_seconds!"
        FROM outbox_events
        WHERE published_at IS NULL
        "#,
    )
    .fetch_one(pool)
    .await?;
    gauge!("chainweave_outbox_unpublished_count").set(row.unpublished_count as f64);
    gauge!("chainweave_outbox_unpublished_oldest_age_seconds").set(row.oldest_age_seconds);
    Ok(())
}

fn row_to_message(row: OutboxRow) -> Result<KafkaOutboxMessage, KafkaDispatchError> {
    let block_height =
        u64::try_from(row.block_height).map_err(|_| KafkaDispatchError::InvalidBlockHeight {
            event_id: row.event_id,
            height: row.block_height,
        })?;
    if row.block_hash.len() != 32 {
        return Err(KafkaDispatchError::InvalidBlockHash {
            event_id: row.event_id,
            length: row.block_hash.len(),
        });
    }
    Ok(KafkaOutboxMessage {
        schema_version: 1,
        event_id: row.event_id,
        chain_id: row.chain_id,
        transition_kind: row.event_kind,
        block_hash: hex_bytes(&row.block_hash),
        block_height,
        ordering: KafkaOutboxOrdering {
            event_id: row.event_id,
            block_height,
        },
        payload: row.payload,
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
    output.push_str("0x");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to string cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, env, str::FromStr as _, sync::Arc};

    use chainweave_core::{BlockHash, BlockHeader, ChainTransition};
    use rdkafka::{
        Message,
        consumer::{CommitMode, Consumer, StreamConsumer},
        message::Headers,
        mocking::MockCluster,
    };
    use serde_json::json;
    use sqlx::{
        Row,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use time::OffsetDateTime;

    use crate::{
        BlockStatus, DurableChainBatch, DurableChainEvent, IndexedBlock, PostgresChainWriter,
        RawLog, StatusSource,
    };

    use super::*;

    const TEST_CHAIN_ID: u64 = 31_337;

    #[derive(Debug, Clone, Default)]
    struct RecordingProducer {
        published: Arc<tokio::sync::Mutex<Vec<KafkaOutboxMessage>>>,
    }

    struct TestDb {
        admin_pool: PgPool,
        schema: String,
        writer: PostgresChainWriter,
    }

    impl OutboxProducer for RecordingProducer {
        fn publish<'a>(
            &'a self,
            _topic: &'a str,
            message: &'a KafkaOutboxMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                self.published.lock().await.push(message.clone());
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn failure_before_publish_leaves_row_unpublished() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        seed_outbox(&db.writer).await;
        let producer = RecordingProducer::default();
        let dispatcher = dispatcher(&db, producer.clone());

        let error = dispatcher
            .dispatch_once_with_failure(KafkaDispatchFailurePoint::BeforePublish)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            KafkaDispatchError::InjectedBeforePublish { event_id: 1 }
        ));
        assert!(producer.published.lock().await.is_empty());
        assert_eq!(
            published_event_ids(db.writer.pool()).await,
            Vec::<i64>::new()
        );
        db.cleanup().await;
    }

    #[tokio::test]
    async fn failure_after_ack_retries_same_event_id() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        seed_outbox(&db.writer).await;
        let producer = RecordingProducer::default();
        let dispatcher = dispatcher(&db, producer.clone());

        let error = dispatcher
            .dispatch_once_with_failure(KafkaDispatchFailurePoint::AfterBrokerAckBeforeMark)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            KafkaDispatchError::InjectedAfterAck { event_id: 1 }
        ));
        assert_eq!(
            published_event_ids(db.writer.pool()).await,
            Vec::<i64>::new()
        );

        let outcome = dispatcher.dispatch_once().await.unwrap();
        assert!(matches!(outcome, KafkaDispatchOutcome::Delivered(_)));
        let published = producer.published.lock().await.clone();
        assert_eq!(
            published
                .iter()
                .map(|message| message.event_id)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        assert_eq!(published_event_ids(db.writer.pool()).await, vec![1]);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn restart_during_delivery_preserves_order_without_gaps() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        seed_outbox(&db.writer).await;
        let producer = RecordingProducer::default();
        let first = dispatcher(&db, producer.clone());
        let second = dispatcher(&db, producer.clone());

        first
            .dispatch_once_with_failure(KafkaDispatchFailurePoint::AfterBrokerAckBeforeMark)
            .await
            .unwrap_err();
        for _ in 0..3 {
            second.dispatch_once().await.unwrap();
        }

        let published = producer.published.lock().await.clone();
        assert_eq!(
            published
                .iter()
                .map(|message| message.event_id)
                .collect::<Vec<_>>(),
            vec![1, 1, 2, 3]
        );
        assert_eq!(published_event_ids(db.writer.pool()).await, vec![1, 2, 3]);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn deduplicated_consumer_view_matches_postgres_transition_history() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        seed_outbox(&db.writer).await;
        let producer = RecordingProducer::default();
        let dispatcher = dispatcher(&db, producer.clone());

        dispatcher
            .dispatch_once_with_failure(KafkaDispatchFailurePoint::AfterBrokerAckBeforeMark)
            .await
            .unwrap_err();
        for _ in 0..3 {
            dispatcher.dispatch_once().await.unwrap();
        }

        let published = producer.published.lock().await.clone();
        let mut seen = BTreeSet::new();
        let deduped = published
            .into_iter()
            .filter(|message| seen.insert(message.event_id))
            .map(|message| {
                (
                    message.transition_kind,
                    message.block_hash,
                    message.block_height,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(deduped, postgres_transition_history(&db.writer).await);
        db.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rdkafka_mock_cluster_dispatches_and_demo_consumer_deduplicates() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        seed_outbox(&db.writer).await;

        let topic = format!(
            "chainweave_m6_{}_{}",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        );
        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster.create_topic(&topic, 1, 1).unwrap();
        let config = KafkaDispatcherConfig::new(
            vec![mock_cluster.bootstrap_servers()],
            topic.clone(),
            1_000,
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        let dispatcher =
            KafkaOutboxDispatcher::connect(db.writer.pool().clone(), config.clone()).unwrap();
        dispatcher
            .dispatch_once_with_failure(KafkaDispatchFailurePoint::AfterBrokerAckBeforeMark)
            .await
            .unwrap_err();
        for _ in 0..3 {
            dispatcher.dispatch_once().await.unwrap();
        }

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.join(","))
            .set("group.id", format!("chainweave-m6-{}", std::process::id()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[&topic]).unwrap();

        let mut seen = BTreeSet::new();
        let mut rendered = Vec::new();
        while seen.len() < 3 {
            let message = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
                .await
                .unwrap()
                .unwrap();
            let headers = message.headers().unwrap();
            assert!(headers.iter().any(|header| header.key == "event_id"));
            assert_eq!(message.key(), Some(b"chain:31337".as_slice()));
            let payload: Value = serde_json::from_slice(message.payload().unwrap()).unwrap();
            let event_id = payload["event_id"].as_i64().unwrap();
            if seen.insert(event_id) {
                rendered.push(render_demo_consumer_event(&payload));
            }
            consumer.commit_message(&message, CommitMode::Sync).unwrap();
        }

        assert_eq!(
            seen.into_iter().collect::<Vec<_>>(),
            vec![1_i64, 2_i64, 3_i64]
        );
        assert!(rendered.iter().any(|line| line.contains("decoded_events=")));
        assert_eq!(published_event_ids(db.writer.pool()).await, vec![1, 2, 3]);
        db.cleanup().await;
    }

    fn dispatcher(
        db: &TestDb,
        producer: RecordingProducer,
    ) -> KafkaOutboxDispatcher<RecordingProducer> {
        KafkaOutboxDispatcher::new(
            db.writer.pool().clone(),
            "chainweave.outbox".to_owned(),
            producer,
            Duration::from_millis(10),
        )
    }

    async fn seed_outbox(writer: &PostgresChainWriter) {
        writer.ensure_chain_identity(hash(90)).await.unwrap();
        writer
            .apply_batch(&DurableChainBatch {
                transition: ChainTransition::Bootstrap,
                common_ancestor: None,
                events: vec![
                    DurableChainEvent::Apply(block(0, 0)),
                    DurableChainEvent::Apply(block(1, 0)),
                    DurableChainEvent::Apply(block(2, 1)),
                ],
            })
            .await
            .unwrap();
    }

    async fn published_event_ids(pool: &PgPool) -> Vec<i64> {
        sqlx::query(
            r"
            SELECT event_id
            FROM outbox_events
            WHERE published_at IS NOT NULL
            ORDER BY event_id
            ",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("event_id").unwrap())
        .collect()
    }

    async fn postgres_transition_history(
        writer: &PostgresChainWriter,
    ) -> Vec<(String, String, u64)> {
        sqlx::query(
            r"
            SELECT event_kind, block_hash, block_height
            FROM outbox_events
            ORDER BY event_id
            ",
        )
        .fetch_all(writer.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            let height = u64::try_from(row.try_get::<i64, _>("block_height").unwrap()).unwrap();
            (
                row.try_get("event_kind").unwrap(),
                hex_bytes(&row.try_get::<Vec<u8>, _>("block_hash").unwrap()),
                height,
            )
        })
        .collect()
    }

    impl TestDb {
        async fn create() -> Option<Self> {
            let Some(database_url) = postgres_database_url() else {
                return None;
            };
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&database_url)
                .await
                .unwrap();
            let schema = format!(
                "chainweave_kafka_test_{}_{}",
                std::process::id(),
                OffsetDateTime::now_utc().unix_timestamp_nanos()
            );
            sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
                .execute(&admin_pool)
                .await
                .unwrap();
            let pool = pool_for_schema(&database_url, &schema).await.unwrap();
            let writer = PostgresChainWriter::new(pool, TEST_CHAIN_ID).unwrap();
            writer.run_migrations().await.unwrap();
            Some(Self {
                admin_pool,
                schema,
                writer,
            })
        }

        async fn cleanup(self) {
            sqlx::query(&format!(r#"DROP SCHEMA "{}" CASCADE"#, self.schema))
                .execute(&self.admin_pool)
                .await
                .unwrap();
        }
    }

    async fn pool_for_schema(database_url: &str, schema: &str) -> Result<PgPool, sqlx::Error> {
        let options = PgConnectOptions::from_str(database_url)
            .unwrap()
            .options([("search_path", schema)]);
        PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
    }

    fn postgres_database_url() -> Option<String> {
        match env::var("CHAINWEAVE_TEST_DATABASE_URL") {
            Ok(database_url) => Some(database_url),
            Err(_) if postgres_tests_required() => {
                panic!(
                    "CHAINWEAVE_TEST_DATABASE_URL must be set when Postgres tests are required; run `make test-postgres-state` for the required Postgres suite"
                );
            }
            Err(_) => {
                eprintln!(
                    "skipping Kafka/Postgres integration test: CHAINWEAVE_TEST_DATABASE_URL unset; run `make test-postgres-state` for the required Postgres suite"
                );
                None
            }
        }
    }

    fn postgres_tests_required() -> bool {
        env::var("CHAINWEAVE_REQUIRE_POSTGRES_TESTS").is_ok_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
    }

    fn block(value: u8, parent: u8) -> IndexedBlock {
        let header = header(value, parent);
        IndexedBlock {
            header,
            timestamp: OffsetDateTime::from_unix_timestamp(1_800_000_000 + i64::from(value))
                .unwrap(),
            status: BlockStatus::Unsafe,
            status_source: StatusSource::Observed,
            logs: vec![RawLog {
                transaction_index: 0,
                log_index: 0,
                tx_hash: hash(40 + value),
                address: [20 + value; 20],
                topics: vec![hash(70 + value)],
                data: vec![value],
                decoded_event: Some(json!({
                    "event": "Transfer",
                    "value": value.to_string(),
                })),
                decoder_version: Some("test:v1".to_owned()),
            }],
        }
    }

    fn header(value: u8, parent: u8) -> BlockHeader {
        BlockHeader::new(hash(value), hash(parent), u64::from(value))
    }

    fn hash(value: u8) -> BlockHash {
        [value; 32]
    }
}
