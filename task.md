# Walter-Python: 地理分布式事务键值存储系统（实验复现简化版）

## 1. 项目概述

**目标**：用 Python 复现 Walter 论文（SOSP 2011）的核心机制与实验场景，用于验证 PSI 一致性模型及性能特性。

**简化原则**：
- ✅ **保留**：PSI 一致性语义、Fast/Slow Commit 协议、向量时间戳、异步复制逻辑、Cset 数据类型、论文实验配置（4 站点延迟矩阵）。
- ❌ **移除**：持久化存储（WAL/Checkpoint）、Configuration Service（Paxos）、故障恢复逻辑、真正的网络部署、课堂演示脚本。
- 🔄 **替代**：单机多进程模拟多站点、内存字典替代数据库、`time.sleep()` 模拟广域网延迟、脚本化指标收集替代 GUI 演示。

**适用对象**：
- 希望验证 Walter 核心协议（PSI、Preferred Site、Cset）正确性的研究者。
- 需要复现论文 Section 8 实验趋势（延迟、吞吐量）的实验环境。

---

## 2. 系统设计说明

为了让未阅读论文的人（或 AI）也能理解本系统，以下是核心设计逻辑的文字描述：

### 2.1 系统架构
Walter 是一个地理分布式的键值存储系统。在真实场景中，它部署在多个数据中心（Site）。在本简化版中，我们在**一台主机上启动 4 个独立的 Python 进程**，每个进程代表一个数据中心（Site）。

- **进程隔离**：每个进程拥有独立的内存空间，模拟物理隔离的数据中心。
- **通信方式**：进程间通过 `localhost` 的 TCP Socket 通信。虽然物理距离为 0，但我们通过在发送/接收代码中插入 `time.sleep()` 来模拟真实的广域网延迟。
- **客户端**：实验脚本作为客户端向任意站点发起事务请求，并记录延迟和吞吐量。

### 2.2 PSI 实现原理详解

Walter 的核心创新在于如何在保证数据一致性的前提下实现异步复制。以下是 PSI 在系统中的具体实现机制及关键名词解释：

#### 2.2.1 并行快照隔离 (PSI)
PSI 是传统快照隔离（Snapshot Isolation, SI）的 relax 版本，专为地理分布式系统设计。
*   **站点内强一致性**：在同一站点内，事务看到的快照是一致的，且提交顺序是固定的。
*   **跨站点因果顺序**：不同站点之间不要求全局统一的提交顺序，但必须保证**因果顺序**。例如，如果事务 B 依赖于事务 A 的结果（B 读取了 A 的写入），那么在任何站点，A 必须在 B 之前提交。
*   **无写写冲突**：PSI 保证任何两个并发提交的事务不会修改同一个对象，从而消除了冲突解决的需求。

#### 2.2.2 首选站点 (Preferred Site)
为了减少跨站点协调，Walter 为每个对象（或容器）分配一个**首选站点**。
*   **定义**：对象最常被修改的站点（例如用户登录的数据中心）。
*   **作用**：当事务在该站点修改该对象时，可以直接使用**快速提交**协议，无需询问其他站点。
*   **灵活性**：对象可以在任何站点被读取，甚至被写入（但非首选站点写入会触发慢速提交）。

#### 2.2.3 快速提交 (Fast Commit)
这是 Walter 性能优化的关键路径。
*   **触发条件**：事务修改的所有常规对象的首选站点都是当前站点，或者事务仅修改 **cset** 对象。
*   **执行流程**：
    1.  **本地冲突检查**：检查对象自事务开始以来是否在本地被修改过。
    2.  **分配序列号**：本地分配一个单调递增的序列号。
    3.  **更新历史**：将写入应用到本地对象历史（History）。
    4.  **异步传播**：提交后立即返回成功，后台线程负责将更新传播到其他站点。
*   **优势**：无跨站点 RPC 延迟，提交速度极快（通常 < 30ms）。

#### 2.2.4 慢速提交 (Slow Commit)
当事务涉及跨站点写入时使用。
*   **触发条件**：事务修改的常规对象中，至少有一个对象的首选站点是远程站点。
*   **执行流程**：
    1.  **协调者角色**：当前站点作为协调者。
    2.  **两阶段提交 (2PC)**：向所有涉及对象的首选站点发送 `Prepare` 请求。
    3.  **投票与锁定**：远程站点检查冲突并锁定对象。如果所有站点投票 `YES`，则提交；否则 abort。
    4.  **提交与释放**：提交后释放锁，并异步传播。
*   **代价**：涉及跨站点网络往返，延迟较高（取决于最远首选站点的 RTT）。

#### 2.2.5 计数集合 (Counting Sets, Csets)
一种特殊的数据类型，用于彻底消除特定场景下的冲突。
*   **结构**：类似集合，但每个元素有一个整数计数（count）。
*   **操作**：`add(x)` 使 count+1，`del(x)` 使 count-1。
*   **特性**：加减法是可交换的（Commutative）。无论操作顺序如何，最终结果一致。
*   **优势**：对 cset 的修改**永远不会产生写写冲突**，因此即使跨站点修改 cset，也可以使用**快速提交**，无需协调。常用于好友列表、消息墙等场景。

#### 2.2.6 异步传播 (Asynchronous Propagation)
事务提交后的数据同步机制。
*   **机制**：提交站点后台批量发送更新到其他站点。
*   **因果顺序保证**：接收站点在应用更新前，必须确保该事务依赖的所有因果前驱事务（根据 **向量时间戳 VTS** 判断）已经到达并提交。
*   **耐久性级别**：
    *   **本地耐久**：本地日志落盘。
    *   **灾难安全耐久**：更新已复制到 `f+1` 个站点。
    *   **全局可见**：更新已提交到所有站点。

#### 2.2.7 向量时间戳 (Vector Timestamps, VTS)
用于替代全局时钟，跟踪因果依赖和快照版本。
*   **结构**：一个向量，每个元素代表一个站点的最新提交序列号。例如 `[2, 5, 3]` 表示站点 1 可见 2 号事务，站点 2 可见 5 号事务。
*   **用途**：
    *   **快照读取**：事务开始时捕获当前 VTS，确保读取一致性。
    *   **可见性判断**：只有当对象的版本序列号 <= VTS 中对应站点的值时，该版本才对事务可见。
    *   **因果检查**：传播时检查 VTS，确保不违反因果顺序。

---

## 3. 实验配置 (基于论文 Section 8.1)

为了复现论文中的实验效果，我们将配置 4 个站点，并使用论文中 Amazon EC2 的真实延迟数据来设置 `sleep` 时间。

### 3.1 站点定义
| 站点 ID | 代号 | 地理位置 | 端口 |
| :--- | :--- | :--- | :--- |
| 0 | VA | Virginia (美国东部) | 5001 |
| 1 | CA | California (美国西部) | 5002 |
| 2 | IE | Ireland (欧洲) | 5003 |
| 3 | SG | Singapore (新加坡) | 5004 |

### 3.2 网络延迟矩阵 (RTT in ms)
我们在 Socket 发送消息前，根据下表 `sleep` 相应的时间（单位：秒）。注意：论文中是往返延迟 (RTT)，单向传播可近似为 RTT/2，但为了简化演示效果，我们建议在 RPC 调用中模拟单向延迟或往返延迟均可，此处建议模拟**单向传播延迟**以便观察复制过程。

*注：下表数据源自论文 Table 8.1 实验设置，已转换为秒供 Python 使用。*

```python
# 论文原文 RTT 数据 (ms): VA-CA 82, VA-IE 87, VA-SG 261, CA-IE 153, CA-SG 190, IE-SG 277
# 此处为了实验复现，直接使用论文 RTT 数据 (秒)，这样实验结果趋势更接近论文。
LATENCY_MATRIX_RTT = {
    #      VA      CA      IE      SG
    VA: [ 0.000,  0.082,  0.087,  0.261 ],
    CA: [ 0.082,  0.000,  0.153,  0.190 ],
    IE: [ 0.087,  0.153,  0.000,  0.277 ],
    SG: [ 0.261,  0.190,  0.277,  0.000 ],
}
```

### 3.3 首选站点 (Preferred Site) 配置
为了演示 Fast Commit，我们需要预设哪些数据属于哪个站点。
```python
PREFERRED_SITES = {
    'user_va_profile': 0,  # VA 用户数据首选 VA
    'user_sg_profile': 3,  # SG 用户数据首选 SG
    'global_timeline': 0,  # 全局时间线首选 VA (通常用 Cset)
}
```

---

## 4. 核心任务清单 (含实现参照伪代码)

### 任务 1：基础数据结构与通信 (3 小时)
实现向量时间戳、事务对象及进程间通信封装。

- **VectorTimestamp**: 字典 `{site_id: seq_no}`，支持比较操作（用于因果检查）。
- **Transaction**: 包含 `tid`, `start_vts`, `updates`, `status`。
- **RPC Wrapper**: 封装 `socket` 发送/接收，并在发送前根据 `LATENCY_MATRIX` 执行 `time.sleep()`。
- **验收标准**：4 个进程能启动，互相 Ping 通，且延迟符合配置矩阵。

```python
# walter_types.py (参照)
from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class VectorTimestamp:
    clocks: Dict[int, int] = field(default_factory=dict)
    def __getitem__(self, site_id: int) -> int:
        return self.clocks.get(site_id, 0)
    def __setitem__(self, site_id: int, value: int):
        self.clocks[site_id] = value

@dataclass
class Transaction:
    tid: int
    start_vts: VectorTimestamp
    updates: List[tuple]  # (oid, op_type, data)
    status: str = "ACTIVE"
```

### 任务 2：单站点 Server 核心逻辑 (4 小时)
实现论文 Figure 10 的本地事务执行逻辑。

- **Memory Store**: `dict` 存储对象，支持多版本（`{oid: [(value, version), ...]}`）。
- **Version Check**: 实现 `is_visible(version, start_vts)` 逻辑，确保快照读。
- **验收标准**：单进程内能正确读写数据，旧版本可见性判断正确。

```python
# walter_server.py (参照)
class WalterServer:
    def __init__(self, site_id: int, preferred_sites: Dict[str, int]):
        self.site_id = site_id
        self.preferred_sites = preferred_sites
        self.curr_seq_no = 0
        self.committed_vts = VectorTimestamp()
        self.got_vts = VectorTimestamp()
        self.history: Dict[str, List] = {}  # oid -> [(data, version)]
        self.locks: Dict[str, int] = {}  # oid -> tid
        
    def start_tx(self) -> Transaction:
        """论文 Figure 10: startTx"""
        tx = Transaction(tid=generate_tid())
        tx.start_vts = self.committed_vts.copy()
        return tx
    
    def read(self, tx: Transaction, oid: str) -> any:
        """论文 Figure 10: read - 基于 start_vts 的快照读"""
        # 实现逻辑：检查 tx.updates 和 self.history 中可见的最新版本
        pass
    
    def write(self, tx: Transaction, oid: str, data: any):
        """缓冲写入，不直接落盘"""
        tx.updates.append((oid, "WRITE", data))
    
    def commit(self, tx: Transaction) -> bool:
        """论文 Figure 11, 12: 根据 preferred site 选择 Fast/Slow Commit"""
        if self._is_fast_commit(tx):
            return self._fast_commit(tx)
        else:
            return self._slow_commit(tx)
```

### 任务 3：Fast Commit 协议 (4 小时)
实现论文 Figure 11 的快速提交逻辑。

- **冲突检查**：检查写集对象自 `start_vts` 后是否被修改。
- **本地提交**：分配本地序列号，更新 `CommittedVTS`。
- **异步传播**：提交后启动后台线程调用 `propagate()`。
- **验收标准**：本地提交延迟 < 10ms（无睡眠），冲突检测正确。

```python
    def _fast_commit(self, tx: Transaction) -> bool:
        """论文 Figure 11: Fast Commit"""
        # 1. 冲突检查：对象自 start_vts 以来是否被修改
        for oid, op_type, _ in tx.updates:
            if op_type == "WRITE":
                if not self._is_unmodified(oid, tx.start_vts):
                    return False
                if oid in self.locks:
                    return False
        
        # 2. 分配序列号并应用更新
        self.curr_seq_no += 1
        tx.version = Version(self.site_id, self.curr_seq_no)
        self._apply_updates(tx.updates, tx.version)
        
        # 3. 更新 CommittedVTS
        self.committed_vts[self.site_id] = tx.version.seq_no
        
        # 4. 异步传播（启动后台线程）
        threading.Thread(target=self._propagate, args=(tx,), daemon=True).start()
        
        return True
```

### 任务 4：Slow Commit 协议 (4 小时)
实现论文 Figure 12 的两阶段提交逻辑。

- **Coordinator**: 识别写集中涉及的非本地首选站点。
- **Prepare Phase**: 向远程站点发送 Prepare 请求（带延迟）。
- **Commit/Abort**: 收集投票，决定提交或中止。
- **验收标准**：跨站点写入能正确协调，冲突时能 abort。

```python
    async def _slow_commit(self, tx: Transaction) -> bool:
        """论文 Figure 12: Slow Commit (2PC)"""
        # 1. 确定涉及的首选站点
        sites = set()
        for oid, op_type, _ in tx.updates:
            if op_type == "WRITE":
                sites.add(self.preferred_sites.get(oid, self.site_id))
        
        # 2. Phase 1: Prepare (并行 RPC)
        votes = await asyncio.gather(*[
            self._remote_prepare(s, tx) for s in sites if s != self.site_id
        ])
        
        if all(votes):
            # 3. Phase 2: Commit
            self.curr_seq_no += 1
            tx.version = Version(self.site_id, self.curr_seq_no)
            self._apply_updates(tx.updates, tx.version)
            self.committed_vts[self.site_id] = tx.version.seq_no
            self._release_locks(tx.tid)
            
            threading.Thread(target=self._propagate, args=(tx,), daemon=True).start()
            return True
        else:
            # 4. Abort
            await asyncio.gather(*[
                self._remote_abort(s, tx.tid) for s in sites if s != self.site_id
            ])
            return False
```

### 任务 5：异步复制与因果顺序 (5 小时)
实现论文 Figure 13 的复制逻辑，确保因果顺序。

- **Propagate Thread**: 后台发送更新到其他站点。
- **Causal Check**: 接收站点检查 `GotVTS` 和 `start_vts`，若依赖未到达则缓存等待。
- **验收标准**：远程站点能按因果顺序应用更新，不会乱序。

```python
    def _propagate(self, tx: Transaction):
        """论文 Figure 13: 异步传播"""
        for site_id in SITE_CONFIG:
            if site_id == self.site_id:
                continue
            
            # 模拟网络延迟
            time.sleep(SITE_CONFIG[self.site_id]['latency_to'][site_id])
            
            # 发送 PROPAGATE 消息
            msg = {
                'type': 'PROPAGATE',
                'tx': {
                    'site_id': self.site_id,
                    'seq_no': tx.version.seq_no,
                    'start_vts': tx.start_vts.clocks,
                    'updates': tx.updates
                }
            }
            send_socket_msg(site_id, msg)
    
    def _on_receive_propagate(self, msg, from_site: int):
        """接收远程事务，检查因果顺序"""
        tx = msg['tx']
        
        # 因果顺序检查：等待依赖事务到达
        while not self._causal_deps_satisfied(tx['start_vts']):
            time.sleep(0.01)
        
        # 站点内顺序检查
        while self.got_vts[from_site] != tx['seq_no'] - 1:
            time.sleep(0.01)
        
        # 应用更新
        self._apply_updates(tx['updates'], Version(from_site, tx['seq_no']))
        self.got_vts[from_site] = tx['seq_no']
```

### 任务 6：Counting Set (Cset) 支持 (3 小时)
实现论文 Section 3.3, 3.5 的 cset 数据类型。

- **Cset Object**: 值结构为 `{element_id: count}`。
- **Operations**: `add` (+1), `del` (-1)。
- **Conflict Free**: 标记 Cset 操作永不冲突，始终走 Fast Commit。
- **验收标准**：多站点并发 add/del 同一 Cset，最终状态一致（计数正确）。

```python
    def _apply_cset_update(self, oid: str, op_type: str, element_id: str):
        """Cset: add/del 操作，永远不冲突"""
        current = self._get_cset_count(oid, element_id)
        if op_type == "ADD":
            new_count = current + 1
        elif op_type == "DEL":
            new_count = current - 1
        
        self._record_cset_update(oid, element_id, new_count)
    
    def _read_cset(self, oid: str) -> List[str]:
        """读取时只返回 count > 0 的元素"""
        return [eid for eid, count in self.csets[oid].items() if count > 0]
```

### 任务 7：多进程启动与通信 (2 小时)
实现单机多进程模拟多站点。

- **Launcher**: 主进程负责启动 4 个 Site 进程。
- **Process Management**: 确保进程崩溃后能重启（可选）。
- **验收标准**：3 个进程能正常启动，通过 Socket 通信。

```python
# main.py (参照)
import multiprocessing
import socket

def run_site(site_id):
    server = WalterServer(site_id, PREFERRED_SITES)
    server.start_socket_listener()
    while True:
        time.sleep(1)

if __name__ == '__main__':
    processes = []
    for site_id in [0, 1, 2, 3]: # 4 站点配置
        p = multiprocessing.Process(target=run_site, args=(site_id,))
        p.start()
        processes.append(p)
    
    # 主进程运行实验脚本
    run_experiment_scripts()
    
    for p in processes:
        p.terminate()
```

### 任务 8：实验脚本与指标收集 (6 小时)
编写复现论文 Section 8 实验场景的脚本，重点在于**指标测量**。

- **Latency Measurement**: 记录 `commit()` 开始到返回的时间。
- **Throughput Measurement**: 记录单位时间内成功提交的事务数。
- **Replication Lag**: 记录写入站点提交时刻与远程站点应用时刻的时间差。
- **验收标准**：能生成类似论文 Figure 18, 19, 20 的延迟/吞吐量数据表。

---

## 5. 论文实验复现计划 (基于 Section 8)

本系统旨在复现 Walter 论文中的关键实验趋势。由于是单机模拟，绝对数值可能与 EC2 不同，但**相对趋势**应保持一致。

### 5.1 实验 1：Fast Commit 基础性能 (对应论文 8.3)
- **目的**：验证本地提交的低延迟特性。
- **配置**：1 到 4 个站点，对象首选站点均匀分布。
- **操作**：客户端向本地站点发起写事务（对象首选站点=本地）。
- **测量指标**：
  - **Commit Latency**: 提交延迟的 CDF 分布（预期 99.9% < 50ms 趋势）。
  - **Throughput**: 随站点数量增加的吞吐量变化（预期线性增长趋势）。
- **预期结果**：延迟主要由本地锁竞争和日志模拟开销决定，不受跨站点 RTT 影响。

### 5.2 实验 2：Slow Commit 协调开销 (对应论文 8.5)
- **目的**：验证跨站点写入的延迟代价。
- **配置**：4 个站点，事务写集包含不同首选站点的对象。
- **操作**：客户端向 VA 站点发起写事务，对象首选站点分别为 VA, CA, IE, SG。
- **测量指标**：
  - **Commit Latency vs WriteSet Size**: 随写集对象数量增加（涉及更多站点），延迟应增加。
  - **预期结果**：延迟 ≈ 2 * (VA 到最远首选站点的 RTT)。例如写 VA+SG 对象，延迟应接近 261ms。

### 5.3 实验 3：Cset 无冲突性能 (对应论文 8.4)
- **目的**：验证 Cset 对象即使跨站点也能走 Fast Commit。
- **配置**：4 个站点，对象为 Cset 类型，首选站点任意。
- **操作**：多站点并发对同一 Cset 进行 `add` 操作。
- **测量指标**：
  - **Throughput**: 对比 Regular Object 跨站点写（Slow Commit）的吞吐量。
  - **预期结果**：Cset 吞吐量应显著高于 Regular Object 跨站点写，接近本地 Fast Commit 水平。

### 5.4 实验 4：异步复制延迟 (对应论文 8.3 Disaster-safe durability)
- **目的**：验证数据传播到远程站点的时间。
- **配置**：4 个站点，VA 写入，观察 SG 收到时刻。
- **测量指标**：
  - **Replication Lag**: `SG 收到时刻 - VA 提交时刻`。
  - **预期结果**：延迟分布在 `[RTT_max, 2 * RTT_max]` 之间（因为论文中提到批量传播机制）。

### 5.5 实验 5：应用级基准 (对应论文 8.6 WaltSocial)
- **目的**：验证典型社交网络操作的性能。
- **配置**：模拟 WaltSocial 数据结构（Profile + FriendList Cset）。
- **操作**：
  - `read-info`: 只读事务。
  - `befriend`: 写两个 Cset 对象。
  - `post-message`: 写 Regular + Cset。
- **测量指标**：各操作的吞吐量 (Kops/s)。
- **预期结果**：`read-info` 最高，`befriend` 次之（全 Cset Fast Commit），`post-message` 再次。

---

## 6. 预期工作量与时间线

| 任务 | 预计时间 | 优先级 | 备注 |
| :--- | :--- | :--- | :--- |
| 任务 1: 基础结构与通信 | 3 小时 | ⭐⭐⭐ | 通信延迟注入是关键 |
| 任务 2: 存储引擎 | 4 小时 | ⭐⭐⭐ | 内存多版本控制 |
| 任务 3: Fast Commit | 4 小时 | ⭐⭐⭐ | 核心性能路径 |
| 任务 4: Slow Commit | 4 小时 | ⭐⭐ | 演示跨站点协调代价 |
| 任务 5: 异步复制 | 5 小时 | ⭐⭐⭐ | 因果顺序保证 |
| 任务 6: Cset 支持 | 3 小时 | ⭐⭐ | 演示无冲突写 |
| 任务 7: 多进程启动 | 2 小时 | ⭐⭐⭐ | 实验环境搭建 |
| 任务 8: 实验脚本 | 6 小时 | ⭐⭐⭐ | **核心产出：数据图表** |
| **总计** | **31 小时** | | 约 1 周兼职开发 |

---

## 7. 快速开始命令

```bash
# 1. 环境准备
python3 --version  # 建议 3.8+

# 2. 启动 4 个站点 (打开 4 个终端)
python walter_server.py --site-id 0  # VA
python walter_server.py --site-id 1  # CA
python walter_server.py --site-id 2  # IE
python walter_server.py --site-id 3  # SG

# 3. 运行实验脚本 (第 5 个终端)
python experiment_fast_commit.py  # 复现 Figure 18
python experiment_slow_commit.py  # 复现 Figure 20
python experiment_cset.py         # 复现 Section 8.4
```

---

## 8. 参考资料

- **论文**: Sovran, Y., et al. "Transactional storage for geo-replicated systems." SOSP 2011.
- **核心算法**: 论文 Figure 9 (Variables), Figure 11 (Fast Commit), Figure 13 (Replication).
- **实验数据**: 论文 Section 8.1 (Experimental setup) & Table 8.1 (Latencies).
- **实现参照**: 本任务清单中的伪代码块。