# walter configuration service implementation
#
# 模拟 Walter 系统中的 Configuration Service（配置服务）。
#
# 根据论文描述：
#   Walter employs a separate configuration service to keep track of the
#   currently active sites, and the preferred site and replica set for each
#   object container. The configuration service tolerates failures by running
#   as a Paxos-based state machine replicated across multiple sites. A Walter
#   server confirms its role in the system by obtaining a lease from the
#   configuration service. The lease assigns a set of containers to a preferred
#   site, and it is held by the Walter server at that site. A Walter server
#   caches the mapping from a container to its replica sites to avoid contacting
#   the configuration service at each access. Incorrect cache entries do not
#   affect correctness because a server rejects requests for which it does not
#   hold the corresponding preferred site lease.
#
# 本模块实现三个核心类：
#   1. Lease           — 租约对象，将一组容器绑定到某个首选站点
#   2. ConfigurationService — 配置服务（模拟 Paxos 状态机），管理站点、容器、租约
#   3. ConfigurationClient  — 嵌入 Walter Server 的客户端，维护本地缓存

import time
import threading
import uuid
from typing import Dict, List, Optional, Set


# ═══════════════════════════════════════════════════════════════════
#  Lease — 租约
# ═══════════════════════════════════════════════════════════════════

class Lease:
    """
    租约对象。
    配置服务向某个站点颁发租约，授权其作为一组容器的首选站点。
    租约有有效期（duration），过期后需要续约，否则该站点失去首选权。
    """

    def __init__(self, lease_id: str, site_id: int,
                 containers: Set[str], duration: float = 30.0):
        """
        Parameters
        ----------
        lease_id   : 租约的唯一标识
        site_id    : 持有租约的站点 ID
        containers : 该租约覆盖的容器（对象 oid）集合
        duration   : 租约有效时长（秒），默认 30 秒
        """
        self.lease_id = lease_id
        self.site_id = site_id
        self.containers: Set[str] = set(containers)
        self.duration = duration
        self.granted_at: float = time.time()        # 颁发时刻
        self.expires_at: float = self.granted_at + duration

    # ---------- 查询 ----------
    def is_expired(self) -> bool:
        """租约是否已过期"""
        return time.time() > self.expires_at

    def is_valid(self) -> bool:
        """租约是否仍然有效（未过期）"""
        return not self.is_expired()

    def covers(self, container_id: str) -> bool:
        """租约是否覆盖指定容器"""
        return container_id in self.containers

    def remaining(self) -> float:
        """距过期还剩多少秒（<=0 表示已过期）"""
        return self.expires_at - time.time()

    # ---------- 续约 ----------
    def renew(self, duration: Optional[float] = None):
        """续约：刷新过期时间"""
        self.granted_at = time.time()
        self.duration = duration if duration is not None else self.duration
        self.expires_at = self.granted_at + self.duration

    def __repr__(self):
        status = "VALID" if self.is_valid() else "EXPIRED"
        return (f"Lease(id={self.lease_id}, site={self.site_id}, "
                f"containers={self.containers}, status={status}, "
                f"remaining={self.remaining():.1f}s)")


# ═══════════════════════════════════════════════════════════════════
#  PaxosLog — 简化的 Paxos 日志（模拟共识）
# ═══════════════════════════════════════════════════════════════════

class PaxosLog:
    """
    简化的 Paxos 提交日志，模拟配置服务在多副本间达成共识的过程。

    在真实系统中，PaxosLog 通过多数派投票保证每条日志条目被大多数
    副本接受后才提交。此处用单节点 + 锁来模拟，但保留了 proposal /
    accept / commit 三阶段接口，方便后续扩展为真正的多副本实现。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._log: List[dict] = []          # 已提交的日志条目
        self._next_proposal_id: int = 0

    def propose(self, operation: dict) -> int:
        """
        提出一个操作提案，返回 proposal_id。
        在简化实现中，提案总是被接受并立即提交。
        """
        with self._lock:
            proposal_id = self._next_proposal_id
            self._next_proposal_id += 1
            entry = {
                "proposal_id": proposal_id,
                "operation": operation,
                "status": "COMMITTED",
                "timestamp": time.time(),
            }
            self._log.append(entry)
            return proposal_id

    def get_committed_entries(self) -> List[dict]:
        """返回所有已提交的日志条目的副本"""
        with self._lock:
            return list(self._log)

    def __len__(self):
        with self._lock:
            return len(self._log)


# ═══════════════════════════════════════════════════════════════════
#  ConfigurationService — 配置服务（模拟 Paxos 状态机）
# ═══════════════════════════════════════════════════════════════════

class ConfigurationService:
    """
    Walter 的配置服务。

    职责：
      1. 跟踪当前所有活跃站点（active sites）。
      2. 维护每个容器（container / oid）的首选站点和副本集。
      3. 通过租约（Lease）将容器分配给首选站点。
      4. 容忍故障 —— 内部使用 Paxos 日志记录所有状态变更。
    """

    def __init__(self, lease_duration: float = 30.0):
        """
        Parameters
        ----------
        lease_duration : 默认租约时长（秒）
        """
        # ---- Paxos 日志 ----
        self._paxos = PaxosLog()

        # ---- 核心状态（由 Paxos 日志驱动） ----
        self._lock = threading.Lock()

        # 活跃站点集合：site_id -> 站点元信息
        self._active_sites: Dict[int, dict] = {}

        # 容器元信息：container_id -> { "preferred_site", "replica_set" }
        self._containers: Dict[str, dict] = {}

        # 当前有效租约：lease_id -> Lease
        self._leases: Dict[str, Lease] = {}

        # 快速索引：site_id -> 该站点持有的所有 lease_id
        self._site_leases: Dict[int, Set[str]] = {}

        self._default_lease_duration = lease_duration

    # ──────────────────── 站点管理 ────────────────────

    def register_site(self, site_id: int, address: str) -> bool:
        """
        注册（或重新激活）一个站点。

        Parameters
        ----------
        site_id : 站点 ID
        address : 站点的网络地址，如 "http://127.0.0.1:5000"

        Returns
        -------
        bool : 注册成功返回 True
        """
        op = {"type": "REGISTER_SITE", "site_id": site_id, "address": address}
        self._paxos.propose(op)

        with self._lock:
            self._active_sites[site_id] = {
                "site_id": site_id,
                "address": address,
                "registered_at": time.time(),
            }
            if site_id not in self._site_leases:
                self._site_leases[site_id] = set()
        return True

    def deregister_site(self, site_id: int) -> bool:
        """
        注销一个站点。该站点持有的所有租约将被撤销。

        Returns
        -------
        bool : 注销成功返回 True；站点不存在返回 False
        """
        op = {"type": "DEREGISTER_SITE", "site_id": site_id}
        self._paxos.propose(op)

        with self._lock:
            if site_id not in self._active_sites:
                return False
            # 撤销该站点的所有租约
            for lease_id in list(self._site_leases.get(site_id, [])):
                self._revoke_lease_unlocked(lease_id)
            del self._active_sites[site_id]
            self._site_leases.pop(site_id, None)
        return True

    def get_active_sites(self) -> Dict[int, dict]:
        """返回当前所有活跃站点的快照"""
        with self._lock:
            return dict(self._active_sites)

    def is_site_active(self, site_id: int) -> bool:
        with self._lock:
            return site_id in self._active_sites

    # ──────────────────── 容器管理 ────────────────────

    def register_container(self, container_id: str,
                           preferred_site: int,
                           replica_sites: List[int]) -> bool:
        """
        注册一个新的容器（对象），并指定首选站点和副本集。

        Parameters
        ----------
        container_id   : 容器/对象 ID（如 "ssA01"）
        preferred_site : 首选站点 ID
        replica_sites  : 副本站点 ID 列表（应包含 preferred_site）

        Returns
        -------
        bool : 注册成功返回 True
        """
        # 确保首选站点在副本集中
        replica_set = set(replica_sites)
        replica_set.add(preferred_site)

        op = {
            "type": "REGISTER_CONTAINER",
            "container_id": container_id,
            "preferred_site": preferred_site,
            "replica_set": list(replica_set),
        }
        self._paxos.propose(op)

        with self._lock:
            self._containers[container_id] = {
                "container_id": container_id,
                "preferred_site": preferred_site,
                "replica_set": replica_set,
            }
        return True

    def get_container_info(self, container_id: str) -> Optional[dict]:
        """查询容器的首选站点和副本集"""
        with self._lock:
            info = self._containers.get(container_id)
            return dict(info) if info else None

    def get_preferred_site(self, container_id: str) -> Optional[int]:
        """查询容器的首选站点 ID"""
        with self._lock:
            info = self._containers.get(container_id)
            return info["preferred_site"] if info else None

    def get_replica_set(self, container_id: str) -> Optional[Set[int]]:
        """查询容器的副本集"""
        with self._lock:
            info = self._containers.get(container_id)
            return set(info["replica_set"]) if info else None

    def update_preferred_site(self, container_id: str,
                              new_preferred_site: int) -> bool:
        """
        变更容器的首选站点。旧首选站点对应的租约会被撤销。

        Returns
        -------
        bool : 变更成功返回 True；容器不存在返回 False
        """
        op = {
            "type": "UPDATE_PREFERRED_SITE",
            "container_id": container_id,
            "new_preferred_site": new_preferred_site,
        }
        self._paxos.propose(op)

        with self._lock:
            info = self._containers.get(container_id)
            if info is None:
                return False
            old_preferred = info["preferred_site"]
            info["preferred_site"] = new_preferred_site
            info["replica_set"].add(new_preferred_site)

            # 撤销旧站点中覆盖此容器的租约
            for lease_id in list(self._site_leases.get(old_preferred, [])):
                lease = self._leases.get(lease_id)
                if lease and container_id in lease.containers:
                    lease.containers.discard(container_id)
                    if not lease.containers:
                        self._revoke_lease_unlocked(lease_id)
        return True

    # ──────────────────── 租约管理 ────────────────────

    def grant_lease(self, site_id: int,
                    containers: Set[str],
                    duration: Optional[float] = None) -> Optional[Lease]:
        """
        向指定站点授予租约，将一组容器绑定为其首选。

        只有当站点处于活跃状态，且每个容器的首选站点确实是该站点时，
        才能授予租约。

        Parameters
        ----------
        site_id    : 申请租约的站点 ID
        containers : 要绑定的容器集合
        duration   : 租约时长（秒），None 则使用默认值

        Returns
        -------
        Lease | None : 成功返回 Lease 对象；失败返回 None
        """
        dur = duration if duration is not None else self._default_lease_duration

        with self._lock:
            # 站点必须活跃
            if site_id not in self._active_sites:
                return None

            # 每个容器的 preferred_site 必须是该站点
            for cid in containers:
                info = self._containers.get(cid)
                if info is None or info["preferred_site"] != site_id:
                    return None

            # 检查是否已有其他站点持有覆盖这些容器的有效租约
            for cid in containers:
                existing_lease = self._find_valid_lease_for_container_unlocked(cid)
                if existing_lease and existing_lease.site_id != site_id:
                    return None  # 另一个站点持有有效租约，拒绝

            lease_id = str(uuid.uuid4())
            lease = Lease(lease_id, site_id, containers, dur)

            op = {
                "type": "GRANT_LEASE",
                "lease_id": lease_id,
                "site_id": site_id,
                "containers": list(containers),
                "duration": dur,
            }
            # 在锁内记日志（简化实现；真实 Paxos 需要先共识再应用）
            self._paxos.propose(op)

            self._leases[lease_id] = lease
            self._site_leases.setdefault(site_id, set()).add(lease_id)
            return lease

    def renew_lease(self, lease_id: str,
                    duration: Optional[float] = None) -> bool:
        """
        续约。只有持有站点仍处于活跃状态时才允许续约。

        Returns
        -------
        bool : 续约成功返回 True
        """
        with self._lock:
            lease = self._leases.get(lease_id)
            if lease is None:
                return False
            if lease.site_id not in self._active_sites:
                return False

            op = {"type": "RENEW_LEASE", "lease_id": lease_id}
            self._paxos.propose(op)

            lease.renew(duration)
        return True

    def revoke_lease(self, lease_id: str) -> bool:
        """
        显式撤销租约。

        Returns
        -------
        bool : 撤销成功返回 True；租约不存在返回 False
        """
        op = {"type": "REVOKE_LEASE", "lease_id": lease_id}
        self._paxos.propose(op)

        with self._lock:
            return self._revoke_lease_unlocked(lease_id)

    def check_lease(self, site_id: int, container_id: str) -> bool:
        """
        检查某站点是否持有覆盖指定容器的有效租约。

        Walter Server 在处理写请求之前调用此方法（或使用本地缓存的
        等价判断）来确认自己是否是该容器的首选站点。

        Returns
        -------
        bool : 是 → True，否 → False
        """
        with self._lock:
            for lid in self._site_leases.get(site_id, []):
                lease = self._leases.get(lid)
                if lease and lease.is_valid() and lease.covers(container_id):
                    return True
            return False

    def get_leases_for_site(self, site_id: int) -> List[Lease]:
        """返回指定站点当前持有的所有租约（含已过期的）"""
        with self._lock:
            return [
                self._leases[lid]
                for lid in self._site_leases.get(site_id, [])
                if lid in self._leases
            ]

    # ---- 内部辅助（需要在已持有 _lock 时调用） ----

    def _revoke_lease_unlocked(self, lease_id: str) -> bool:
        lease = self._leases.pop(lease_id, None)
        if lease is None:
            return False
        self._site_leases.get(lease.site_id, set()).discard(lease_id)
        return True

    def _find_valid_lease_for_container_unlocked(self, container_id: str) -> Optional[Lease]:
        """在所有租约中找到覆盖此容器且仍有效的租约"""
        for lease in self._leases.values():
            if lease.is_valid() and lease.covers(container_id):
                return lease
        return None

    # ──────────────────── 过期清理 ────────────────────

    def cleanup_expired_leases(self):
        """
        清理所有已过期的租约。可由后台定时任务周期性调用。
        """
        with self._lock:
            expired_ids = [
                lid for lid, lease in self._leases.items()
                if lease.is_expired()
            ]
            for lid in expired_ids:
                self._revoke_lease_unlocked(lid)
            return len(expired_ids)

    # ──────────────────── 状态快照 ────────────────────

    def snapshot(self) -> dict:
        """返回配置服务的完整状态快照，用于调试或持久化"""
        with self._lock:
            return {
                "active_sites": dict(self._active_sites),
                "containers": {
                    cid: {
                        "preferred_site": info["preferred_site"],
                        "replica_set": list(info["replica_set"]),
                    }
                    for cid, info in self._containers.items()
                },
                "leases": {
                    lid: repr(lease)
                    for lid, lease in self._leases.items()
                },
                "paxos_log_length": len(self._paxos),
            }

    def __repr__(self):
        return (f"ConfigurationService(sites={len(self._active_sites)}, "
                f"containers={len(self._containers)}, "
                f"leases={len(self._leases)})")


# ═══════════════════════════════════════════════════════════════════
#  ConfigurationClient — Walter Server 内嵌的配置客户端（含本地缓存）
# ═══════════════════════════════════════════════════════════════════

class ConfigurationClient:
    """
    嵌入 Walter Server 的配置客户端。

    它与 ConfigurationService 交互以获取/续约租约，并在本地缓存
    容器→副本站点的映射，避免每次访问都联系配置服务。

    论文指出：不正确的缓存条目不影响正确性，因为服务器会拒绝自己
    不持有对应首选站点租约的请求。
    """

    def __init__(self, site_id: int, config_service: ConfigurationService):
        """
        Parameters
        ----------
        site_id        : 本站点 ID
        config_service : 配置服务实例的引用（真实系统中为 RPC stub）
        """
        self.site_id = site_id
        self._config_service = config_service

        # ---- 本地缓存 ----
        self._cache_lock = threading.Lock()

        # container_id -> { "preferred_site": int, "replica_set": set[int] }
        self._container_cache: Dict[str, dict] = {}

        # 本站点当前持有的租约
        self._my_leases: Dict[str, Lease] = {}

    # ──────────────────── 租约操作 ────────────────────

    def acquire_lease(self, containers: Set[str],
                      duration: Optional[float] = None) -> Optional[Lease]:
        """
        向配置服务申请租约。成功后更新本地缓存。

        Returns
        -------
        Lease | None
        """
        lease = self._config_service.grant_lease(
            self.site_id, containers, duration
        )
        if lease:
            with self._cache_lock:
                self._my_leases[lease.lease_id] = lease
                # 刷新缓存
                for cid in containers:
                    self._refresh_container_cache_unlocked(cid)
        return lease

    def renew_lease(self, lease_id: str,
                    duration: Optional[float] = None) -> bool:
        """续约"""
        ok = self._config_service.renew_lease(lease_id, duration)
        if ok:
            with self._cache_lock:
                if lease_id in self._my_leases:
                    self._my_leases[lease_id].renew(duration)
        return ok

    def release_lease(self, lease_id: str) -> bool:
        """主动释放租约"""
        ok = self._config_service.revoke_lease(lease_id)
        with self._cache_lock:
            self._my_leases.pop(lease_id, None)
        return ok

    # ──────────────────── 缓存查询 ────────────────────

    def get_preferred_site(self, container_id: str) -> Optional[int]:
        """
        查询容器的首选站点，优先从缓存返回。
        缓存未命中时联系配置服务并更新缓存。
        """
        with self._cache_lock:
            cached = self._container_cache.get(container_id)
            if cached is not None:
                return cached["preferred_site"]

        # 缓存未命中 → 联系配置服务
        info = self._config_service.get_container_info(container_id)
        if info is None:
            return None

        with self._cache_lock:
            self._container_cache[container_id] = {
                "preferred_site": info["preferred_site"],
                "replica_set": set(info["replica_set"]),
            }
        return info["preferred_site"]
    
    def get_preferred_site_url(self, container_id: str):
        info = self._config_service.get_container_info(container_id)
        if info is None:
            return None
        preferred_site_id = info["preferred_site"]
        active_sites = self._config_service.get_active_sites()
        preferred_site_info = active_sites.get(preferred_site_id)
        if preferred_site_info is None:
            return None
        return preferred_site_info["address"]

    def get_replica_set(self, container_id: str) -> Optional[Set[int]]:
        """查询容器的副本集，优先从缓存返回"""
        with self._cache_lock:
            cached = self._container_cache.get(container_id)
            if cached is not None:
                return set(cached["replica_set"])

        info = self._config_service.get_container_info(container_id)
        if info is None:
            return None

        with self._cache_lock:
            self._container_cache[container_id] = {
                "preferred_site": info["preferred_site"],
                "replica_set": set(info["replica_set"]),
            }
        return set(info["replica_set"])

    def is_locally_preferred(self, container_id: str) -> bool:
        """判断容器的首选站点是否是本站点"""
        pref = self.get_preferred_site(container_id)
        return pref == self.site_id

    def holds_lease_for(self, container_id: str) -> bool:
        """
        判断本站点是否持有覆盖该容器的有效租约。

        这是论文中"A server rejects requests for which it does not hold
        the corresponding preferred site lease"的本地快速判断。
        """
        with self._cache_lock:
            for lease in self._my_leases.values():
                if lease.is_valid() and lease.covers(container_id):
                    return True
        return False

    # ──────────────────── 缓存管理 ────────────────────

    def invalidate_cache(self, container_id: Optional[str] = None):
        """
        使缓存失效。

        Parameters
        ----------
        container_id : 指定容器 ID 则只失效该条目；None 则清空全部缓存
        """
        with self._cache_lock:
            if container_id is None:
                self._container_cache.clear()
            else:
                self._container_cache.pop(container_id, None)

    def refresh_cache(self, container_id: str):
        """强制从配置服务刷新指定容器的缓存"""
        info = self._config_service.get_container_info(container_id)
        if info:
            with self._cache_lock:
                self._container_cache[container_id] = {
                    "preferred_site": info["preferred_site"],
                    "replica_set": set(info["replica_set"]),
                }

    def _refresh_container_cache_unlocked(self, container_id: str):
        """（已持有 _cache_lock 时调用）刷新单条缓存"""
        info = self._config_service.get_container_info(container_id)
        if info:
            self._container_cache[container_id] = {
                "preferred_site": info["preferred_site"],
                "replica_set": set(info["replica_set"]),
            }

    # ──────────────────── 状态查看 ────────────────────

    def cache_snapshot(self) -> dict:
        """返回本地缓存的快照，用于调试"""
        with self._cache_lock:
            return {
                "container_cache": {
                    cid: {
                        "preferred_site": v["preferred_site"],
                        "replica_set": list(v["replica_set"]),
                    }
                    for cid, v in self._container_cache.items()
                },
                "my_leases": {
                    lid: repr(lease)
                    for lid, lease in self._my_leases.items()
                },
            }

    def __repr__(self):
        return (f"ConfigurationClient(site={self.site_id}, "
                f"cached={len(self._container_cache)}, "
                f"leases={len(self._my_leases)})")
