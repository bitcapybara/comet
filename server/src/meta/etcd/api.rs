use etcd::{Compare, CompareOp, PutOptions, TxnOp};
use etcd_client as etcd;

/// 如果不存在，插入新 kv，如果已存在，返回存在的kv
#[tracing::instrument(skip(client, value))]
pub async fn put_if_absent(
    mut client: etcd::Client,
    key: &str,
    value: &[u8],
    lease_id: i64,
) -> Result<Option<Vec<u8>>, etcd::Error> {
    let txn = etcd::Txn::new()
        .when([Compare::create_revision(key, CompareOp::Equal, 0)])
        .and_then([TxnOp::put(
            key,
            value,
            Some(PutOptions::new().with_lease(lease_id)),
        )])
        .or_else([TxnOp::get(key, None)]);
    let resp = client.txn(txn).await?;
    if resp.succeeded() {
        return Ok(None);
    }
    let op_responses = &resp.op_responses();
    let Some(etcd::TxnOpResponse::Get(resp)) = op_responses.first() else {
        unreachable!()
    };
    let Some(kv) = resp.kvs().first() else {
        unreachable!()
    };
    Ok(Some(kv.value().to_vec()))
}
