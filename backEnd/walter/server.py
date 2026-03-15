# walter server implementation

import json
import time
import threading
import multiprocessing
import requests
from backEnd.walter.configuration import ConfigurationClient
from backEnd.walter.transaction import Transaction


class Server:
    def __init__(self, total_server_num, config_client: ConfigurationClient):
        self.currentSeqNo       = 0                          # 当前的序列编号
        self.committedVTS       = [0] * total_server_num     # 已提交的版本时间戳
        self.gotVTS             = [0] * total_server_num     # 已获得的版本时间戳
        self.history            = {}                         # oid: [[op, <site,seqno>], ...]
        self.config_client      = config_client
        self.siteID             = config_client.site_id      # 本站点 ID
        self.thread_lock        = threading.Lock()           # 快/慢提交全局锁
        self.object_locks       = {}                         # 慢提交对象锁 {oid: tid}
        self.object_locks_mutex = threading.Lock()           # 保护 object_locks

    # ──────────────── 内部辅助 ──────────────────────────────────────────── #

    def _get_site_url(self, site_id: int) -> str:
        """返回指定站点的 HTTP base URL"""
        active = self.config_client._config_service.get_active_sites()
        return active[site_id]["address"]

    def _get_other_site_ids(self) -> list:
        """返回除本站点之外所有活跃站点的 ID 列表"""
        active = self.config_client._config_service.get_active_sites()
        return [sid for sid in active if sid != self.siteID]

    def _build_site_url_map(self) -> dict:
        """返回 {site_id: url} 映射（供传播进程使用）"""
        active = self.config_client._config_service.get_active_sites()
        return {sid: info["address"] for sid, info in active.items()}

    # ──────────────── 执行事务 ──────────────────────────────────────────── #

    def starTx(self) -> Transaction:
        return Transaction().create(self.committedVTS)

    def write(self, x: Transaction, oid, data):
        """写一个对象"""
        x.add_update(['WRITE', oid, data])
        return None

    def setAdd(self, x: Transaction, setid, elem):
        """集合加"""
        x.add_update(['SET_ADD', setid, elem])
        return None

    def setDel(self, x: Transaction, setid, elem):
        """集合减"""
        x.add_update(['SET_DEL', setid, elem])
        return None

    def read(self, x: Transaction, oid):
        """读取一个对象"""
        states = [s for s in x.updates if s[1] == oid]
        hiss   = self.history_VTS_visible(oid, x.startVTS)

        if self.config_client.is_locally_preferred(oid):
            data = states[-1][2] if states else hiss[-1][0][2]
            return data
        else:
            siteUrl   = self.config_client.get_preferred_site_url(oid)
            res       = requests.post(siteUrl + "/history", json={"oid": oid, "VTS": x.startVTS})
            site_hiss = json.loads(res.text).get('data', [])
            if states:
                return states[-1][2]
            elif site_hiss:
                return site_hiss[-1][0][2]
            else:
                return hiss[-1][0][2]

    def setRead(self, x: Transaction, setid):
        """集合读"""
        states = [s for s in x.updates if s[1] == setid]
        hiss   = self.history_VTS_visible(setid, x.startVTS)

        if not self.config_client.is_locally_preferred(setid):
            siteUrl   = self.config_client.get_preferred_site_url(setid)
            res       = requests.post(siteUrl + "/history", json={"oid": setid, "VTS": x.startVTS})
            site_hiss = json.loads(res.text).get('data', [])
            hiss     += site_hiss

        data = {}
        for state in states:
            elem = state[2]
            op   = state[0]
            data.setdefault(elem, 0)
            if op == "SET_ADD":
                data[elem] += 1
            elif op == "SET_DEL":
                data[elem] -= 1
        for his in hiss:
            elem = his[0][2]
            op   = his[0][0]
            data.setdefault(elem, 0)
            if op == "SET_ADD":
                data[elem] += 1
            elif op == "SET_DEL":
                data[elem] -= 1
        return data

    # ──────────────── 事务提交 ──────────────────────────────────────────── #

    def unmodified(self, oid, VTS) -> bool:
        """对象自 VTS 以来没被修改过"""
        if oid not in self.history:
            return True
        for his in self.history[oid]:
            his_ID, seq = his[1]
            if VTS[his_ID] < seq:
                print("对象被修改过，当前VTS:", VTS, "在此之后的修改记录：", his)
                return False
        return True

    def update(self, updates: list, version: tuple):
        """将一批更新写入 history"""
        for upd in updates:
            oid = upd[1]
            self.history.setdefault(oid, [])
            self.history[oid].append([upd, version])

    def prepare_lock(self, tid: str, oids: list, startVTS: list, retList: list):
        """慢提交投票：尝试锁定 oids；结果（True/False）追加到 retList"""
        with self.object_locks_mutex:
            for oid in oids:
                if not self.unmodified(oid, startVTS) or oid in self.object_locks:
                    retList.append(False)
                    return
            for oid in oids:
                self.object_locks[oid] = tid
        retList.append(True)

    def abort_unlock(self, tid: str):
        """释放属于 tid 的所有对象锁"""
        with self.object_locks_mutex:
            to_remove = [oid for oid, t in self.object_locks.items() if t == tid]
            for oid in to_remove:
                del self.object_locks[oid]

    def fastCommit(self, x: Transaction):
        """快提交（写集合全在本地首选站点）"""
        print("\n  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[线程] 快提交━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
        print("  ┃ ", x)

        self.thread_lock.acquire()
        for oid in x.writeset.keys():
            if not self.unmodified(oid, x.startVTS) or oid in self.object_locks:
                self.thread_lock.release()
                print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━× ABORTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
                x.outcome = "ABORTED"
                return x.outcome

        print("  ┃ ▶ 开始更新History     ", end="")
        self.currentSeqNo += 1
        x.seqno = self.currentSeqNo
        self.update(x.updates, (self.siteID, x.seqno))
        print("  ┃ ☑ 更新结束")
        self.thread_lock.release()

        print("  ┃ ▶ 等待提交排序        ", end="")
        while self.committedVTS[self.siteID] < x.seqno - 1:
            time.sleep(0.2)
        self.committedVTS[self.siteID] = x.seqno
        print("  ┃ ☑ 提交结束")

        x.outcome = "COMMITTED"
        print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━√ COMMITTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

        p = multiprocessing.Process(
            target=_propagate_worker,
            args=(self._get_other_site_ids(), self._build_site_url_map(), self.siteID, _tx_to_dict(x))
        )
        p.start()
        return x.outcome

    def slowCommit(self, x: Transaction):
        """慢提交（写集合中有非本站点首选的对象）"""
        print("\n  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[线程] 慢提交━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
        print("  ┃ ", x)

        # 按首选站点分组写对象
        sites = {}
        for oid in x.writeset.keys():
            pref = self.config_client.get_preferred_site(oid)
            sites.setdefault(pref, []).append(oid)
        print("  ┃   要写入的站点：", sites)

        # 各站点投票
        votes = {}
        for site, oids in sites.items():
            print("  ┃▲  请求投票 {} 站点:{}".format(oids, site))
            if site != self.siteID:
                serverUrl = self._get_site_url(site)
                res  = requests.post(serverUrl + "/prepare", json={
                    "tid": x.tid, "oids": oids,
                    "startVTS": x.startVTS, "id": self.siteID
                })
                vote = json.loads(res.text).get("status")
            else:
                retList = []
                self.prepare_lock(x.tid, oids, x.startVTS, retList)
                vote = "YES" if retList[0] else "NO"
            print("  ┃    ▷", vote)
            votes[site] = (vote == "YES")
        print("  ┃   ★ 投票结果", votes)

        if all(votes.values()):
            self.thread_lock.acquire()
            print("  ┃ ▶ 开始更新History    ", end="")
            self.currentSeqNo += 1
            x.seqno = self.currentSeqNo
            self.update(x.updates, (self.siteID, x.seqno))
            print("  ┃ ☑ 更新结束")
            self.thread_lock.release()

            print("  ┃ ▶ 等待提交排序        ", end="")
            while self.committedVTS[self.siteID] < x.seqno - 1:
                time.sleep(0.2)
            self.committedVTS[self.siteID] = x.seqno
            print("  ┃ ☑ 等待结束")

            print("  ┃ ▶ 释放本地加锁        ", end="")
            self.abort_unlock(x.tid)
            print("  ┃ ☑ 释放结束")

            x.outcome = "COMMITTED"
            print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━√ COMMITTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

            p = multiprocessing.Process(
                target=_propagate_worker,
                args=(self._get_other_site_ids(), self._build_site_url_map(), self.siteID, _tx_to_dict(x))
            )
            p.start()
            return x.outcome
        else:
            print("  ┃ ▶ 解锁            ", end="")
            for site, voted_yes in votes.items():
                if voted_yes and site != self.siteID:
                    serverUrl = self._get_site_url(site)
                    requests.post(serverUrl + "/abort", json={"tid": x.tid, "id": self.siteID})
            print("  ┃ ☑ 释放结束")
            print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━× ABORTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
            x.outcome = "ABORTED"
            return x.outcome

    def commitTx(self, x: Transaction):
        """提交事务"""
        x.writeset = {}
        for upd in x.updates:
            if upd[0] == 'WRITE':
                x.writeset[upd[1]] = upd

        print("[3.1] ---------获得写集合---------")
        print(x.writeset)

        for oid in x.writeset.keys():
            if self.config_client.get_preferred_site(oid) != self.siteID:
                print("[3.2] ---------慢提交--------------")
                com = threading.Thread(target=self.slowCommit, args=(x,))
                com.start()
                com.join()
                print("==================================[事务 {}]======================================\n\n".format(x.outcome))
                return

        print("[3.2] ---------快提交--------------")
        com = threading.Thread(target=self.fastCommit, args=(x,))
        com.start()
        com.join()
        print("==================================[事务 {}]======================================\n\n".format(x.outcome))

    # ──────────────── history 处理 ──────────────────────────────────────── #

    def history_VTS_visible(self, oid, VTS) -> list:
        """返回 oid 的历史中，VTS 可见的记录"""
        hiss = []
        if oid in self.history:
            for his in self.history[oid]:
                siteId, seq = his[1]
                if VTS[siteId] >= seq:
                    hiss.append(his)
        return hiss

    def get_local_history(self):
        myHis, otherHis = {}, {}
        for key, value in self.history.items():
            if self.config_client.is_locally_preferred(key):
                myHis[key] = value
            else:
                otherHis[key] = value
        return myHis, otherHis


# ══════════════════════════════════════════════════════════════════════
#  模块级辅助函数（multiprocessing 子进程不能序列化 self）
# ══════════════════════════════════════════════════════════════════════

def _tx_to_dict(x: Transaction) -> dict:
    """将事务中需要传播的字段打包为普通 dict"""
    return {
        "tid":      x.tid,
        "seqno":    getattr(x, 'seqno', None),
        "startVTS": x.startVTS,
        "updates":  x.updates,
        "outcome":  getattr(x, 'outcome', None),
        "writeset": {k: v for k, v in getattr(x, 'writeset', {}).items()},
    }


def _propagate_worker(other_site_ids: list, site_url_map: dict, my_site_id: int, x_dict: dict):
    """
    在独立进程中执行传播逻辑。
    不依赖 self，所有数据通过参数传入。
    """
    print("╭────────────────────────────────[进程] 同步传播─────────────────────────────────────╮")
    print("│ ", x_dict)
    print("│ --------------------------------------1. 传播--------------------------------------------------------")
    for sid in other_site_ids:
        serverUrl = site_url_map[sid]
        print("│▲  同步到 站点 {} URL:{}".format(sid, serverUrl))
        try:
            res = requests.post(serverUrl + "/propagate", json={"x": x_dict, "id": my_site_id})
            print("|    ▷", res.text.replace("\n", ""))
            if json.loads(res.text).get("status") == "ERROR":
                print("╰─────────────────────────────────────────────────────────────────────────────────────╯")
                return
        except Exception as e:
            print("|    ▷ 连接失败:", e)
            return

    print("| ♢事务现在 是 disaster-safe durable")
    x_dict['mark'] = "disaster-safe durable"
    print("│ ---------------------------------------2. 灾难安全备份------------------------------------------------")
    for sid in other_site_ids:
        serverUrl = site_url_map[sid]
        print("│▲  同步到 站点 {} URL:{}".format(sid, serverUrl))
        try:
            res = requests.post(serverUrl + "/ds_durable", json={"x": x_dict, "id": my_site_id})
            print("|    ▷", res.text.replace("\n", ""))
        except Exception as e:
            print("|    ▷ 连接失败:", e)

    print("| ♢事务现在 是 globally visible")
    x_dict['mark'] = "globally visible"
    print("╰─────────────────────────────────────────────────────────────────────────────────────╯")
