# 阿里云 MaxCompute

## 简介

`flusher_aliyun_odps` 将日志写入阿里云 MaxCompute。仅支持 v1 Pipeline 的日志数据。

## 版本

[Alpha](../../stability-level.md)，待发布。

## 配置参数

| 参数 | 类型 | 必选 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| Type | String | 是 | — | `flusher_aliyun_odps` |
| Endpoint | String | 是 | — | MaxCompute API 端点（包含 `/api`），不是 Tunnel 端点；插件从服务端获取 Tunnel 地址。必须包含 `http://` 或 `https://`，推荐 HTTPS |
| AccessKeyID | String | 是 | — | 访问目标项目的访问密钥 ID；使用 STS 时填临时 AccessKey ID |
| AccessKeySecret | String | 是 | — | 与 AccessKeyID 配对的 Secret；使用 STS 时填临时 AccessKey Secret |
| SecurityToken | String | 否 | 空 | 使用 STS 临时凭据时填写，必须与临时 AK 配套；留空时使用 AK 认证。插件不自动续期，到期前需更新整套凭据 |
| ProjectName | String | 是 | — | 已存在的目标 MaxCompute 项目名，与 Endpoint 所在地域匹配 |
| TableName | String | 是 | — | 目标表名，不包含项目或 Schema 前缀；表须提前创建 |
| SchemaName | String | 否 | 空 | 目标 Schema 名，通过独立参数传递；留空时不向 SDK 显式指定 Schema |
| PartitionConfig | String | 否 | 空 | 目标分区表达式，支持固定值、日志字段和时间模板；详见下文。非分区表留空，分区表需配置 |
| TimeRange | Integer | 否 | 15 | 时间模板的对齐粒度，单位分钟，不是发送间隔；PartitionConfig 非空时必须为 15 的正整数倍，且不超过 71582788 |

## 字段与分区

目标表必须预先创建。日志字段按列名映射，未提供的列写 NULL；多余字段忽略。
字符串空值保持为空字符串，其他类型空值转换为 NULL。类型转换错误的记录被丢弃并告警。

## 采集元信息（__extra_info__）

当目标表包含名为 `__extra_info__` 的 JSON 列时，插件自动填充采集元信息，
当前默认内容为 `{"host_ip":"采集器主机 IP"}`；HostIP 为空时该键省略。
该列由插件保留：同名业务字段不会写入，也不用于收集未映射的业务字段。
没有该列时不注入元信息；同名列为 STRING 等非 JSON 类型时按普通业务字段映射。
旧名称 `__dwarf_info__` 不再具有特殊含义，按普通字段映射。

内部 level 2 分支还会填充 hostname、collect_time（日志时间，Unix 秒）和
flush_time（记录转换时刻，Unix 毫秒）；当前默认 level 1，没有对外配置入口。

CHAR(n)/VARCHAR(n) 使用目标表声明的 n 构造 SDK 值，超长记录丢弃，不自动截断。
当前使用的 Go SDK 按 UTF-8 字节长度校验，例如中文字符通常占 3 字节。

## PartitionConfig：目标分区规则

格式为 `分区列名=分区值`，多个分区列用逗号连接。左侧是目标表的分区列名，
右侧可以使用固定文本或 `{{...}}` 模板；目标表的分区列必须与生成的表达式匹配。
插件按每条日志生成目标分区，创建上传 session 时请求自动创建尚不存在的分区，
因此所用账号需要相应的写入及创建分区权限；插件不会自动创建项目或表。

### 固定值、日志字段和组合

| 写法 | 含义 | 结果示例 |
| --- | --- | --- |
| `env=prod` | 所有记录写入固定分区 | `env=prod` |
| `region={{region}}` | 读取日志中名为 region 的字段 | 字段值为 `cn-hangzhou` 时得到 `region=cn-hangzhou` |
| `region={{source_region}}` | 分区列名与日志字段名可以不同 | source_region 为 `cn-hangzhou` 时得到 `region=cn-hangzhou` |
| `app=service_{{app}}` | 固定文本与字段值拼接 | app 为 `order` 时得到 `app=service_order` |
| `env=prod,region={{region}}` | 多列分区 | `env=prod,region=cn-hangzhou` |
| `ds={{%Y%m%d}},region={{region}}` | 时间模板与日志字段组合 | `ds=20261028,region=cn-hangzhou` |

`{{region}}` 只查找进入 Flusher 时日志的 `Contents` 字段，按字段名精确匹配，
不会读取 LogTags、环境变量或嵌套 JSON 路径。如果原始日志仍只有 `content`，
需要先通过 Processor 提取所引用的字段。字段缺失或值为空时，该条记录发送失败，
同批其他记录仍会继续处理。

模板必须使用双花括号，且不能为空，例如 `{region}`、`{{}}` 都会导致初始化失败。
插件直接拼接字段值，不做分区值转义；字段含逗号、等号等分区语法字符时，
应先在 Processor 中处理，避免生成无效或非预期的分区表达式。

### 时间模板与 TimeRange

以 `%` 开头的模板按时间解析，例如 `{{%Y%m%d}}`。时间来源为日志的
`log.Time`（Unix 秒时间戳），不是发送时的系统时间，也不会自动读取名为 `time`
或 `timestamp` 的普通日志字段。要按业务事件时间分区，需要在上游处理阶段设置日志时间。

计算顺序为：先将 Unix 秒时间戳按 `TimeRange × 60` 秒向下取整，
再按 Agent 进程的本地时区格式化。不提供单独的分区时区配置，部署时需保持 Agent 时区一致。
`TimeRange` 不控制发送频率，也不会等待一个时间窗口结束才发送。

例如 Agent 时区为 Asia/Shanghai，日志时间为 `2026-10-28 15:17:23`：

| PartitionConfig | TimeRange | 生成的分区 |
| --- | --- | --- |
| `ds={{%Y%m%d}}` | 15 | `ds=20261028` |
| `ds={{%Y%m%d}},hh={{%H}},mm={{%M}}` | 15 | `ds=20261028,hh=15,mm=15` |
| `ds={{%Y%m%d}},hh={{%H}},mm={{%M}}` | 30 | `ds=20261028,hh=15,mm=00` |
| `ds={{%Y%m%d}},hh={{%H}}` | 60 | `ds=20261028,hh=15` |

常用时间标记如下：

| 标记 | 含义 | 示例 |
| --- | --- | --- |
| `%Y` / `%y` | 四位 / 两位年份 | `2026` / `26` |
| `%m` / `%d` | 两位月份 / 日期 | `10` / `28` |
| `%H` / `%I` | 24 小时制 / 12 小时制小时 | `15` / `03` |
| `%M` / `%S` | 分钟 / 秒，使用对齐后的时间 | `15` / `00` |
| `%p` | 上午 / 下午 | `AM` / `PM` |
| `%a` / `%A` | 英文星期缩写 / 全称 | `Wed` / `Wednesday` |
| `%b` / `%B` | 英文月份缩写 / 全称 | `Oct` / `October` |
| `%U` | 年内周数，以周日为一周开始，范围 00–53 | `43` |
| `%W` | 年内周数，以周一为一周开始，范围 00–53 | `43` |
| `%j` | 年内第几天，范围 001–366 | `301` |

同一个时间模板内只能连续拼接时间标记：`{{%Y%m%d}}` 合法，
`{{%Y-%m-%d}}` 不合法；如需连字符，写成 `{{%Y}}-{{%m}}-{{%d}}`。
`%U`、`%W` 中，每年第一个周日、周一之前的日期分别属于第 `00` 周，
不采用 ISO 周编号，也不会沿用上一年的周数。例如 `2025-01-01` 的 `%U`、`%W`
均为 `00`，`%j` 为 `001`；闰年 `2024-12-31` 的 `%j` 为 `366`。

只要 `PartitionConfig` 非空，即使只使用固定值或日志字段，`TimeRange` 也会被校验；
保留默认值 15 即可。留空表示不指定目标分区，不会自动根据表结构推导分区。

### 分区缓存

本地最多缓存 128 个分区 session。缓存达到上限后写入新分区时先处理最久未访问分区的待发送数据，再移除本地 session；不会删除云端分区。

## 失败与投递语义

插件采用尽力投递。单条脏数据不会阻止同批正常记录发送。
每批发送最多尝试 3 次，等待间隔为 500ms、1s；单次请求超时 30 秒。
JSON 列要求合法 JSON 文本，按其原始类型写入；整数超出目标列范围时视为脏数据。
同一批中同一分区的 session 创建失败只尝试一次，下一批重新尝试。

ODPS 在内存中保留当前批记录，失败后重建 writer 重放；成功或重试耗尽后释放本批缓冲。
重试耗尽时告警并丢弃失败数据，后续批次可继续发送；不会跨批次保留或持久化失败数据。
网络响应不确定时，重试可能导致重复，不保证 exactly-once。
进程异常退出可能丢失内存中的数据。

## 运行日志

session 创建及本地缓存清理记录 INFO，清理原因区分过期、容量淘汰及发送失败后移除；不会删除云端分区。session 创建失败及每次发送尝试失败记录 ERROR，包含目标资源、耗时及错误原因；同一批次相同分区的 session 创建失败只记录一次。每批发送成功记录 INFO，包含目标资源、分区、TraceID、记录数、字节数、尝试次数和耗时。
字段转换失败日志包含列名、目标类型及失败类别（例如 length_exceeded、out_of_range、invalid_value），不输出凭据或记录原文。

## 样例

以下凭据与项目名均为占位符，替换为具有目标资源写权限的配置。

```yaml
enable: true
inputs:
  - Type: input_file
    FilePaths:
      - /home/test-log/*.log
flushers:
  - Type: flusher_aliyun_odps
    Endpoint: https://service.cn-hangzhou.maxcompute.aliyun.com/api
    AccessKeyID: <access-key-id>
    AccessKeySecret: <access-key-secret>
    ProjectName: project_sample
    TableName: odps_sample
```
