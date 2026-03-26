推荐的最小工程框架

配置与常量层
SiteConfig
职责：站点定义（VA/CA/IE/SG、端口、站点编号映射）。
LatencyMatrix
职责：提供站点间延迟查询，支持 RTT 或单向模式切换。
PreferredSiteTable
职责：对象到首选站点映射，统一查询入口。
MessageType（枚举）
职责：PING、START_TX、READ、COMMIT、PREPARE、VOTE、ABORT、PROPAGATE 等消息类型。

基础数据模型层
VectorTimestamp
职责：向量时钟读写、拷贝、比较（<=、merge、依赖满足判断）。
Version
职责：版本标识（site_id, seq_no）。
UpdateOp
职责：一次写操作描述（oid, op_type, payload）。
Transaction
职责：事务上下文（tid, start_vts, updates, status, begin_ts, commit_ts）。
TxStatus（枚举）
职责：ACTIVE、PREPARED、COMMITTED、ABORTED。

存储与并发控制层
VersionedObjectStore
职责：普通对象多版本历史管理，按 start_vts 做可见版本选择。
CsetStore
职责：维护 cset 计数，支持 add/del 和可见值读取（count > 0）。
LockTable
职责：2PC 期间的对象锁定与释放。
SiteClock
职责：本地单调 seq_no 分配与 committed_vts 更新。

协议核心层
PSIReadEngine
职责：快照读路径（先读事务写缓冲，再读历史可见版本）。
ConflictDetector
职责：判断普通对象是否自 start_vts 后被修改；cset 默认无冲突。
FastCommitEngine
职责：本地冲突检查、分配版本、应用更新、触发异步传播。
SlowCommitCoordinator
职责：2PC 协调（prepare、收票、commit/abort、释放锁）。
PrepareHandler（参与者逻辑）
职责：收到 prepare 后执行冲突检查并加锁返回投票。
ReplicationEngine
职责：提交后异步向其他站点传播事务更新。
CausalApplyQueue
职责：远端传播到达后按因果依赖与站点内顺序进行缓存/应用。
VisibilityGuard
职责：检查 start_vts 依赖是否满足、got_vts 顺序是否连续。

网络与进程层
RpcServer
职责：监听端口，解析消息，路由到对应处理器。
RpcClient
职责：发送 RPC、注入延迟、超时与重试（先简单实现无重试）。
MessageCodec
职责：消息序列化与反序列化（JSON 即可）。
SiteProcess
职责：单站点进程入口，组装 WalterSite 并启动线程。
ClusterLauncher
职责：本机启动多个站点进程并管理生命周期。

实验与指标层
WorkloadClient
职责：生成事务请求并发到指定站点。
MetricsCollector
职责：记录 commit latency、throughput、replication lag。
ExperimentRunner
职责：按实验 1-5 配置运行场景。
ResultExporter
职责：导出 CSV（后续再画图）。
建议的目录骨架

core
protocol
network
site
experiments
scripts
tests
TODO List（按落地顺序）

先打通最小通信闭环
完成 SiteConfig、LatencyMatrix、RpcServer、RpcClient、MessageCodec。
验收：4 站点可互 ping，实测延迟接近矩阵配置。

实现基础数据结构
完成 VectorTimestamp、Version、Transaction、UpdateOp、TxStatus。
验收：可做 VTS 比较、拷贝、依赖检查单测。
实现单站点 MVCC 读写
完成 VersionedObjectStore、PSIReadEngine、SiteClock。
验收：start_vts 快照读正确，历史可见性正确。

实现 Fast Commit 主路径
完成 ConflictDetector、FastCommitEngine、ReplicationEngine（先只打印传播日志）。
验收：本地写提交成功，冲突时 abort，延迟低。

实现 Slow Commit（2PC）
完成 SlowCommitCoordinator、PrepareHandler、LockTable。
验收：跨站点写正常 commit；冲突或否决时 abort 并释放锁。

实现异步复制与因果应用
完成 CausalApplyQueue、VisibilityGuard、got_vts 管理。
验收：乱序到达不会错误应用；依赖满足后可自动推进。

加入 Cset 支持
完成 CsetStore、Cset 操作类型、在冲突检测中豁免。
验收：多站点并发 add/del 最终一致，且无需 slow commit。

实现多进程启动器
完成 SiteProcess、ClusterLauncher、启动/停止脚本。
验收：一条命令启动 4 站点，退出时可清理。

实现实验脚本与指标采集
实验 1 Fast Commit 延迟/吞吐。
实验 2 Slow Commit 延迟随写集变化。
实验 3 Cset 对比普通对象。
实验 4 replication lag 分布。
实验 5 WaltSocial 简化 workload。
验收：产出可复现实验趋势的 CSV 表。

补测试与稳定性
单测：VTS、可见性、冲突检测、2PC 状态机。
集成测：4 站点端到端事务与复制顺序。
验收：核心路径可重复运行，无明显随机失败。