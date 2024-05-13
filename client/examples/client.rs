#![allow(dead_code)]

use std::{
    num::{NonZeroU8, NonZeroUsize},
    path::PathBuf,
};

use comet_client::{
    client::{consumer, producer::PublishMessage, Client, Config},
    dns::LocalDnsResolver,
};
use comet_common::{
    codec::bincode::Bincode,
    http::topic,
    mtls::CertsFile,
    types::{AccessMode, InitialPosition, SubscriptionType},
};
use futures::{SinkExt, TryStreamExt};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace, Resource};
use snafu::Snafu;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[derive(Debug, Snafu)]
enum Error {
    Comet { source: comet_client::Error },
}

#[snafu::report]
#[tokio::main]
async fn main() -> Result<(), comet_client::Error> {
    let span_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");
    let trace_config = trace::config().with_resource(Resource::new(vec![KeyValue::new(
        "service.name",
        "comet_client",
    )]));
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(span_exporter)
        .with_trace_config(trace_config)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("couldn't create OTLP tracer");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(telemetry_layer)
        // .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true)
                .and_then(
                    filter::targets::Targets::new().with_target("comet_client", Level::DEBUG),
                ),
        )
        .init();

    let project_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config = Config::builder(
        vec!["http://localhost:6888".parse().unwrap()],
        CertsFile {
            ca_cert_file: project_path.join("../certs/ca-cert.pem"),
            cert_file: project_path.join("../certs/server-cert.pem"),
            key_file: project_path.join("../certs/server-key.pem"),
        },
    )
    .keepalive(10000)
    .build()
    .await?;

    let client = Client::with_default(config);

    if let Err(e) = process(client.clone()).await {
        println!("process error:{e}")
    }

    client.close().await;

    Ok(())
}

async fn process(client: Client<LocalDnsResolver, Bincode>) -> Result<(), comet_client::Error> {
    client
        .create_topic("test_topic", topic::Config::default())
        .await
        .ok();

    let (handle, stream) = client
        .new_consumer_builder::<String>("test-consumer", "test_topic", "test_subscription")?
        .priority(NonZeroU8::new(5).unwrap())
        .initial_position(InitialPosition::Earliest)
        .subscription_type(SubscriptionType::Exclusive)
        .build()
        .await?;

    let token = CancellationToken::new();
    let join_handle = tokio::spawn(consume_loop(handle.clone(), stream, token.clone()));

    let mut producer = client
        .new_producer_builder::<String>("test-producer", "test_topic")?
        .access_mode(AccessMode::Exclusive)
        .queue_size(NonZeroUsize::new(100).unwrap())
        .build()
        .await?;
    for _ in 0..2 {
        producer.send(PublishMessage::new("hello world")).await?;
    }

    println!("waiting on signal...");
    signal::ctrl_c().await.unwrap();
    token.cancel();

    join_handle.await.expect("consumer panic")?;

    Ok(())
}

async fn consume_loop(
    handle: consumer::ConsumerHandle,
    mut stream: consumer::ConsumerStream<Bincode, String>,
    token: CancellationToken,
) -> Result<(), comet_client::Error> {
    loop {
        tokio::select! {
            res = stream.try_next() => {
                match res {
                    Ok(Some(message)) => {
                        println!("{}: {}", message.id, message.payload);
                        handle.acks([message.id]).await?;
                        println!("acked");
                    },
                    Ok(None) => return Ok(()),
                    Err(e) => Err(e)?
                }
            }
            _ = token.cancelled() => return Ok(())
        }
    }
}

async fn consume_once(
    handle: consumer::ConsumerHandle,
    mut stream: consumer::ConsumerStream<Bincode, String>,
    token: CancellationToken,
) -> Result<(), comet_client::Error> {
    tokio::select! {
        res = stream.try_next() => {
            match res {
                Ok(Some(message)) => {
                    println!("{}: {}", message.id, message.payload);
                    handle.acks([message.id]).await?;
                    println!("acked");
                },
                Ok(None) => return Ok(()),
                Err(e) => Err(e)?
            }
        }
        _ = token.cancelled() => return Ok(())
    }
    Ok(())
}
