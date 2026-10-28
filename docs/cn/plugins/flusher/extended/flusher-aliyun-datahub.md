# 阿里云 DataHub

## 简介

`flusher_aliyun_datahub` 将日志写入阿里云 DataHub。仅支持 v1 Pipeline 的日志数据。

## 版本

[Alpha](../../stability-level.md)，待发布。

## 配置参数

| 参数 | 类型 | 必选 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| Type | String | 是 | — | `flusher_aliyun_datahub` |
| Endpoint | String | 是 | — | 目标 DataHub 项目所在地域的服务端点，必须包含 `http://` 或 `https://`，推荐 HTTPS |
| AccessKeyID | String | 是 | — | 访问目标项目的访问密钥 ID；使用 STS 时填临时 AccessKey ID |
| AccessKeySecret | String | 是 | — | 与 AccessKeyID 配对的 Secret；使用 STS 时填临时 AccessKey Secret |
| SecurityToken | String | 否 | 空 | 使用 STS 临时凭据时填写，必须与临时 AK 配套；留空时使用 AK 认证。插件不自动续期，到期前需更新整套凭据 |
| ProjectName | String | 是 | — | 已存在的 DataHub 项目名，与 Endpoint 所在地域匹配 |
| TopicName | String | 是 | — | 目标项目下已存在的 Topic 名；插件自动读取其 Tuple/BLOB 类型及 Schema，不自动创建 Topic |

## 字段映射

目标 Topic 必须预先创建。Tuple Topic 按日志字段名匹配 Schema，并按字段类型转换；
Schema 中不存在的日志字段会被忽略；无效数值等转换错误只丢弃对应记录，并告警。BLOB Topic 在日志仅包含
`content` 字段时写入该字段原文，否则将日志字段序列化成 JSON。
Tuple 字段名按 Schema 精确匹配：缺失的可空字段写 NULL，缺失的非空字段导致整条记录丢弃；
STRING 的空字符串保留，其他类型的空字符串仅在字段允许 NULL 时写 NULL。
BLOB 的 JSON 路径将字段值保留为字符串，例如 count 字段值 `42` 写成 `{"count":"42"}`。
原始 JSON 日志如需写入 Tuple Topic，应先使用 Processor 提取字段；插件不会自动解析 content 中的 JSON。

## 采集元信息（Attributes）

Tuple 和 BLOB 记录均携带独立的 Attributes，不占用 Tuple Schema 列，也不修改 BLOB 正文。
当前默认写入以下属性，无需在 Topic Schema 中建列：

| 属性 | 来源 | 写入条件 |
| --- | --- | --- |
| host_ip | 采集器全局配置的 HostIP | 默认写入 |
| path | LogGroup.LogTags 中的 `__path__` | 存在该 Tag 时写入 |
| __topic__ | LogGroup.Topic，表示源日志 Topic，不是目标 DataHub Topic | 非空时写入 |

这些属性不会从同名业务字段读取。未映射的业务字段也不会自动转存到 Attributes。
内部 level 2 分支另含 hostname（采集器主机名）、collect_time（log.Time，Unix 秒）和
flush_time（记录转换时刻，Unix 毫秒，不代表服务端写入成功时间）；当前没有对外配置入口，默认不写入这三项。

## 失败与投递语义

插件采用尽力投递。单条脏数据不会阻止同批正常记录发送。
每批发送最多尝试 3 次，等待间隔为 500ms、1s；单次请求超时 30 秒。
DataHub 分片封闭时会刷新分片并额外尝试一次。
已有 schema 的定期刷新失败时暂用缓存，三分钟后再次尝试刷新；首次获取失败则初始化失败。
重试耗尽时告警并丢弃失败数据，后续批次可继续发送；不会跨批次保留或持久化失败数据。
网络响应不确定时，重试可能导致重复，不保证 exactly-once。
进程异常退出可能丢失内存中的数据。

## 运行日志

初始化、Shard 封闭后刷新成功和每批发送成功均记录 INFO；发送成功日志包含目标资源、RequestID、记录数、数据大小和耗时。发送失败记录 ERROR。
转换失败按批次汇总丢弃数量，并记录首个字段转换错误的列名、目标类型和失败类别；日志不输出凭据或记录原文。

## 样例

以下凭据与项目名均为占位符，替换为具有目标资源写权限的配置。

```yaml
enable: true
inputs:
  - Type: input_file
    FilePaths:
      - /home/test-log/*.log
flushers:
  - Type: flusher_aliyun_datahub
    Endpoint: https://dh-cn-hangzhou.aliyuncs.com
    AccessKeyID: <access-key-id>
    AccessKeySecret: <access-key-secret>
    ProjectName: project_sample
    TopicName: datahub_sample
```
