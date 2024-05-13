use snafu::{Location, ResultExt, Snafu};

use crate::addr::ConnectAddress;

use super::{topic, OkResponse};

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("send http request to {url}"))]
    HttpRequest {
        url: String,
        #[snafu(source)]
        error: reqwest::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("receive unexpected status code {status} from {url}, message {message:?}"))]
    HttpServerResponse {
        url: String,
        status: u16,
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("empty addresses"))]
    EmptyAddresses {
        #[snafu(implicit)]
        location: Location,
    },
}

#[tracing::instrument]
pub async fn lookup_topic_addresses(
    http_addrs: &[ConnectAddress],
    topic_name: &str,
) -> Result<Vec<ConnectAddress>, Error> {
    let mut addrs = http_addrs.iter();
    let mut last_err = EmptyAddressesSnafu.build();
    loop {
        match addrs.next() {
            Some(addr) => match lookup_topic_address(addr, topic_name).await {
                Ok(broker_addrs) => break Ok(broker_addrs),
                Err(e) => {
                    last_err = e;
                    continue;
                }
            },
            None => return Err(last_err),
        }
    }
}

#[tracing::instrument]
pub async fn lookup_topic_address(
    addr: &ConnectAddress,
    topic_name: &str,
) -> Result<Vec<ConnectAddress>, Error> {
    let url = &format!("{addr}/comet/api/public/topic/{topic_name}/broker_addresses");
    let resp = reqwest::get(url).await.context(HttpRequestSnafu { url })?;
    if !resp.status().is_success() {
        return HttpServerResponseSnafu {
            url,
            status: resp.status().as_u16(),
            message: resp.text().await.context(HttpRequestSnafu { url })?,
        }
        .fail();
    }

    let body: OkResponse<_> = resp.json().await.context(HttpRequestSnafu { url })?;

    Ok(body.data)
}

#[tracing::instrument]
pub async fn get_server_http_addresses(
    addrs: &[ConnectAddress],
) -> Result<Vec<ConnectAddress>, Error> {
    let mut addrs = addrs.iter();
    let mut last_err = EmptyAddressesSnafu.build();
    loop {
        match addrs.next() {
            Some(addr) => match get_http_addresses_inner(addr).await {
                Ok(http_addrs) => break Ok(http_addrs),
                Err(e) => {
                    last_err = e;
                    continue;
                }
            },
            None => break Err(last_err),
        }
    }
}

#[tracing::instrument]
async fn get_http_addresses_inner(addr: &ConnectAddress) -> Result<Vec<ConnectAddress>, Error> {
    let url = &format!("{addr}/comet/api/public/addresses");
    let resp = reqwest::get(url).await.context(HttpRequestSnafu { url })?;
    if !resp.status().is_success() {
        return HttpServerResponseSnafu {
            url,
            status: resp.status().as_u16(),
            message: resp.text().await.context(HttpRequestSnafu { url })?,
        }
        .fail();
    }

    let body: OkResponse<_> = resp.json().await.context(HttpRequestSnafu { url })?;

    Ok(body.data)
}

#[tracing::instrument]
pub async fn create_topic(
    addrs: &[ConnectAddress],
    topic_name: &str,
    config: topic::Config,
) -> Result<(), Error> {
    let mut addrs = addrs.iter();
    let mut last_err = EmptyAddressesSnafu.build();
    loop {
        match addrs.next() {
            Some(addr) => match create_topic_inner(addr, topic_name, &config).await {
                Ok(_) => break Ok(()),
                Err(e) => {
                    last_err = e;
                    continue;
                }
            },
            None => break Err(last_err),
        }
    }
}

async fn create_topic_inner(
    addr: &ConnectAddress,
    topic_name: &str,
    config: &topic::Config,
) -> Result<(), Error> {
    let url = &format!("{addr}/comet/api/public/topic/{topic_name}");
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .json(&config)
        .send()
        .await
        .context(HttpRequestSnafu { url })?;
    if !resp.status().is_success() {
        return HttpServerResponseSnafu {
            url,
            status: resp.status().as_u16(),
            message: resp.text().await.context(HttpRequestSnafu { url })?,
        }
        .fail();
    }

    Ok(())
}
