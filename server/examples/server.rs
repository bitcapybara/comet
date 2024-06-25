use std::path::PathBuf;

use futures::{stream::FuturesUnordered, StreamExt};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace, Resource};
use tokio::signal::unix::{signal, SignalKind};
use tracing::Level;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};

use comet_common::mtls::CertsFile;

#[tokio::main]
async fn main() {
    let span_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");
    let trace_config = trace::config().with_resource(Resource::new(vec![KeyValue::new(
        "service.name",
        "comet_server",
    )]));
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(span_exporter)
        .with_trace_config(trace_config)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("couldn't create OTLP tracer");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let console_layer = console_subscriber::spawn();

    tracing_subscriber::registry()
        .with(telemetry_layer)
        .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true)
                .and_then(
                    filter::targets::Targets::new()
                        // .with_target("tokio", Level::TRACE)
                        // .with_target("runtime", Level::TRACE),
                        .with_target("comet_server", Level::DEBUG),
                ),
        )
        .init();

    run_server().await;
}

#[cfg(all(
    feature = "local-memory",
    not(any(feature = "local-persist", feature = "distributed"))
))]
async fn run_server() {
    comet_server::start_local_memory_server::<_, comet_common::codec::bincode::Bincode>(
        get_config(),
        signal_shutdown([
            SignalKind::hangup(),
            SignalKind::interrupt(),
            SignalKind::terminate(),
        ]),
    )
    .await
    .unwrap();
}

#[cfg(all(
    feature = "local-memory",
    not(any(feature = "local-persist", feature = "distributed"))
))]
fn get_config() -> comet_server::Config {
    get_server_config()
}

#[cfg(feature = "local-persist")]
async fn run_server() {
    comet_server::start_local_persist_server::<_, comet_common::codec::bincode::Bincode>(
        get_config(),
        signal_shutdown([
            SignalKind::hangup(),
            SignalKind::interrupt(),
            SignalKind::terminate(),
        ]),
    )
    .await
    .expect("run server error")
}

#[cfg(feature = "local-persist")]
fn get_config() -> comet_server::Config {
    let project_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    comet_server::Config {
        server: get_server_config(),
        meta_dir: project_path.join("data"),
        storage_dir: project_path.join("data"),
        storage_mem_message_num_limit: None,
    }
}

#[cfg(feature = "distributed")]
async fn run_server() {
    comet_server::start_distributed_server::<_, comet_common::codec::bincode::Bincode>(
        get_config(),
        signal_shutdown([
            SignalKind::hangup(),
            SignalKind::interrupt(),
            SignalKind::terminate(),
        ]),
    )
    .await
    .expect("run server error")
}

#[cfg(feature = "distributed")]
fn get_config() -> comet_server::Config {
    comet_server::Config {
        node_id: 1,
        server: get_server_config(),
        meta: comet_server::etcd::Config {
            node_id: 1,
            http_addr: "http://localhost:6888".parse().unwrap(),
            broker_addr: "quic://localhost:5888".parse().unwrap(),
            etcd_addrs: vec!["localhost:2379".to_string()],
        },
        storage: comet_server::postgres::Config {
            host: "localhost".to_string(),
            port: 6432,
            user: "postgres".to_string(),
            password: "devpass".to_string(),
            db_name: "cometmq".to_string(),
            mem_messages_num_limit: None,
        },
    }
}

fn get_server_config() -> comet_server::ServerConfig {
    let project_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    comet_server::ServerConfig {
        quic: comet_server::QuicConfig {
            certs: CertsFile {
                ca_cert_file: project_path.join("../certs/ca-cert.pem"),
                cert_file: project_path.join("../certs/server-cert.pem"),
                key_file: project_path.join("../certs/server-key.pem"),
            },
            connect_addr: "quic://localhost:5888".parse().unwrap(),
            listen_addr: "0.0.0.0:5888".parse().unwrap(),
        },
        http: comet_server::HttpConfig {
            connect_addr: "http://localhost:6888".parse().unwrap(),
            listen_addr: "0.0.0.0:6888".parse().unwrap(),
        },
    }
}

async fn signal_shutdown(signals: impl IntoIterator<Item = SignalKind>) {
    let mut futs = FuturesUnordered::new();
    for kind in signals {
        let mut signal = signal(kind).expect("listen on signal error");
        futs.push(async move { signal.recv().await });
    }
    futs.next().await;
}
