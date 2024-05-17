# Comet MQ

受 Pulsar 启发而实现的消息队列库，包含服务端和客户端。

## 主要特性

* 自定义通信协议粘包/拆包，最大化减少消息体大小
* 服务端协议使用 [serde](https://docs.rs/serde/latest/serde/) 库进行序列话/反序列化，目前支持 [bincode](https://docs.rs/bincode/latest/bincode/)/[json](https://docs.rs/serde_json/latest/serde_json/)，用户可以自行扩展指定
* 使用 [`QUIC`](https://github.com/aws/s2n-quic) 实现客户端和服务端协议通信
    * 使用 `stream` 池避免频繁创建
    * 使用 `pipeline` 模式发送消息，提高消息收发的并发量
* 使用`双端 TLS` 认证，保证消息传递的安全性
* 服务端支持三种 `feature` 适应不同部署场景
    * 本地内存模式（`local-memory`）：元数据和消息保存在当前节点内存中，适合“一次性”测试使用
    * 本地持久化模式（`local-persist`）：元数据保存在 [`redb`](https://github.com/cberner/redb) 文件中，消息保存在 [`sqlite`](https://www.sqlite.org/index.html) 文件中，适合非分布式场景的单节点持久化使用
    * 分布式模式（`distributed`）：元数据保存在 [`etcd`](https://etcd.io/) 中，消息保存在 [`PostgreSQL`](https://www.postgresql.org/) 中，适合大规模分布式部署的场景
* 服务端为`无状态节点`，可以任意启动和停止，客户端在服务端的连接断开后会自动迁移到其他节点
* 支持`即时消息`和`延时消息`，客户端 `Producer` 可以通过指定传递时间来决定服务端何时把消息发送给消费者
* 支持通过`数量/TTL`指定已 ack 消息的保留策略
* Topic 支持`分区`，客户端生产者可以将消息轮询发送给多个节点的 Topic 分区来提高消息发送的并发度
* 客户端 `Producer` 实现了 [`Sink trait`](https://docs.rs/futures/latest/futures/sink/trait.Sink.html), `Consumer` 实现了 [`Stream trait`](https://docs.rs/futures/latest/futures/stream/index.html)，便于在流式场景中使用

## Example

1. 安装 [mkcert](https://github.com/FiloSottile/mkcert)

2. 生成 cert 认证文件

```bash
make init
```

3. 启动客户端

```bash
cargo run --example client
```

4. 启动服务端

```bash
# local-memory mode
cargo run --example server --features local-memory

# local-persist mode
cargo run --example server --features local-persist

# distributed mode
cargo run --example server --features distributed
```