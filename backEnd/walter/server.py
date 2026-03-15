# walter server implementation


import json
import threading
import requests
from backEnd.walter.configuration import ConfigurationClient
from backEnd.walter.transaction import Transaction


class Server:
    def __init__(self, total_server_num, config_client: ConfigurationClient):
        self.currentSeqNo = 0     # 当前的序列编号
        self.committedVTS = [0] * total_server_num  # 已提交的版本时间戳
        self.gotVTS = [0] * total_server_num  # 已获得的版本时间戳
        self.history = {}  # 历史记录 结构：# oid： [[ ['WRITE', oid,     data],        <site,seqno>],]
        self.config_client = config_client
        self.thread_lock = threading.Lock()  # 用于快提交的锁
    #-------------------------执行事务-----------------#
    def starTx(self) -> Transaction:
        return Transaction().create(self.committedVTS)

    #写一个对象
    def write(self, x: Transaction, oid, data):
        x.add_update(['WRITE', oid, data])
        return None

    #集合加
    def setAdd(self, x: Transaction, setid, id):
        x.add_update(['SET_ADD', setid, id])
        return None

    #集合减
    def setDel(self, x: Transaction, setid, id):
        x.add_update(['SET_DEL', setid, id])
        return None

    #读取一个对象
    def read(self, x: Transaction, oid):
        if self.config_client.is_locally_preferred(oid):
            # 本地复制的
            # 1.返回x中反应oid的
            states = []
            for s in x.updates:
                if s[1] == oid:
                    states.append(s)
            # 2. 返回历史中最新的提交
            hiss=self.history_VTS_visible(oid,x.startVTS)

            # 对于常规对象 walter 返回x.updates中的最新提交
            # 如果没有 返回 history中最后一次提交
            data=states[-1][2] if len(states)>0 else hiss[-1][0][2]
            return data
        else:
            # 非本地复制的
            # 1.返回x中反应oid的
            states = []
            for s in x.updates:
                if s[1] == oid:
                    states.append(s)
            # 2. 返回历史中最新的提交
            hiss=self.history_VTS_visible(oid,x.startVTS)
            # 3. 返回其主站点中的历史
            siteUrl=self.config_client.get_preferred_site_url(oid)
            # 远程请求
            res=requests.post(siteUrl+"/history",json={"oid":oid,"VTS":x.startVTS})
            site_hiss=json.loads(res.text).get('data')

            # 对于常规对象 walter 返回x.updates中的最新提交
            # 如果没有 返回 history中最后一次提交
            if len(states)>0:
                data=states[-1][2]
            elif len(site_hiss)>0:
                data=site_hiss[-1][0][2]
            else:
                data=hiss[-1][0][2]
            return data

    # 集合读
    def setRead(self,x: Transaction,setid):
        # 1.返回x中反应oid的
        states = []
        for s in x.updates:
            if s[1] == setid:
                states.append(s)
        # 2. 返回历史中最新的提交
        hiss=self.history_VTS_visible(setid,x.startVTS)
        # 3 非本站点 请求返回
        if not self.config_client.is_locally_preferred(setid):
            siteUrl=self.config_client.get_preferred_site_url(setid)
            # 远程请求
            res=requests.post(siteUrl+"/history",json={"oid":setid,"VTS":x.startVTS})
            site_hiss=json.loads(res.text).get('data')
            hiss+=site_hiss
        
        # 解析数据
        data={}
        for state in states:
            id=state[2]
            op=state[0]
            if id not in data.keys():
                data[id]=0
            
            if op == "SET_ADD":
                data[id]+=1
            elif op== "SET_DEL":
                data[id]-=1
        for his in hiss:
            id=state[0][2]
            op=state[0][0]
            if id not in data.keys():
                data[id]=0
            
            if op == "SET_ADD":
                data[id]+=1
            elif op== "SET_DEL":
                data[id]-=1
        return data
    
    #-----------------------事务提交---------------------------
    def unmodified(self, oid, VTS):
        '''对象没被修改过'''
        if oid not in self.history.keys():
            return True
        else:
            for his in self.history[oid]:
                his_ID,seq=his[1] # update中的<siteId(注意不一定是本机id), seqno>
                if VTS[his_ID]<seq:
                    print("对象被修改过，当前VTS:",VTS,"在此之后的修改记录：",his)
                    return False
            return True

    def update(self, updates,version):
        ''' 更新'''
        for update in updates:
            oid=update[1]
            if oid not in self.history.keys():
                self.history[oid]=[]
            self.history[oid].append([update,version])

    def fastCommit(self, x: Transaction):
        ''' 快提交  '''
        print("\n  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[线程] 快提交━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
        print("  ┃ ",x)
        global currentSeqNo

        #加锁
        self.thread_lock.acquire()
        for oid in x['writeset'].keys():
            if self.unmodified(oid, x['startVTS']) and oid not in object_locks.keys():
                continue
            else:
                # 解锁
                ThreadLock.release()
                print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━× ABORTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
                x['outcome'] = "ABORTED"
                return x['outcome']

        # 提交没有冲突 开始更新
        print("  ┃ ▶ 开始更新History     ",end="")
        currentSeqNo +=1
        x['seqno']=currentSeqNo
        update(x['updates'],(siteID,x['seqno']))
        print("  ┃ ☑ 更新结束")
        #解锁
        ThreadLock.release()
        #等待
        print("  ┃ ▶ 等待提交排序        ",end="")
        while CommittedVTS[siteID] < x['seqno'] - 1:
            time.sleep(2)
        CommittedVTS[siteID] = x['seqno']

        print("  ┃ ☑ 提交结束")
        x['outcome'] = "COMMITTED"
        print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━√ COMMITTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

        #进行传播
        p=multiprocessing.Process(target=propagate,args=(x,))
        p.start()
        # p.join()
        return x['outcome']

    def slowCommit(self, x):
        ''' 慢提交 '''
        print("\n  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[线程] 慢提交━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
        print("  ┃ ",x)
        global currentSeqNo

        #获得所有写入对象的首选站点(非本站点)
        sites={}
        for oid in x['writeset'].keys():
            pref_siteID=objects[oid]['prefID']
            if pref_siteID not in sites:
                sites[pref_siteID]=[]
            sites[pref_siteID].append(oid)
        print("  ┃   要写入的站点：",sites)
        #远程调用投票
        votes={}
        for site,oids in sites.items():
            serverUrl=servers[site][3]
            print("  ┃▲  请求投票 {} URL:{}".format(oids,serverUrl))
            if site!=siteID:
                res=requests.post(serverUrl+"/prepare",json={"tid":x['tid'],"oids":oids,"startVTS":x["startVTS"],"id":siteID})
                vote=json.loads(res.text).get("status")
            else:
                retList=[]
                prepare_lock(x['tid'],oids,x["startVTS"],retList)
                vote="YES" if retList[0] else "NO"  
            print("  ┃    ▷",vote)
            votes[site]=(True if vote=="YES" else False)
        print("  ┃   ★ 投票结果",votes)
        #如果所有的投票通过
        if all(list(votes.values())):
            ThreadLock.acquire()
            print("  ┃ ▶ 开始更新History    ",end="")
            currentSeqNo +=1
            x['seqno']=currentSeqNo
            update(x['updates'],(siteID,x['seqno']))
            print("  ┃ ☑ 更新结束")
            ThreadLock.release()
            print("  ┃ ▶ 等待提交排序        ",end="")
            while CommittedVTS[siteID] < x['seqno'] - 1:
                time.sleep(2)
            CommittedVTS[siteID] = x['seqno']
            print("  ┃ ☑ 等待结束")
            print("  ┃ ▶ 释放本地加锁        ",end="")
            abort_unlock(x["tid"])
            print("  ┃ ☑ 释放结束")
            x['outcome'] = "COMMITTED"
            print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━√ COMMITTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

            #进行传播
            p=multiprocessing.Process(target=propagate,args=(x,))
            p.start()
            # p.join()
            return x['outcome']
        else:
            #投票失败 解锁
            print("  ┃ ▶ 解锁            ",end="")
            for site,vote in votes.items():
                if vote and site!=siteID:
                    serverUrl=servers[site][3]
                    res=requests.post(serverUrl+"/abort",json={"tid":x['tid'],"id":siteID})
            print("  ┃ ☑ 释放结束")
            print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━× ABORTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
            x['outcome'] = "ABORTED"
            return x['outcome']

    def commitTx(self, x: Transaction):
        ''' 提交事务'''
        #获得写集合
        x['writeset'] = {}
        for update in x['updates']:
            if update[0] in ['WRITE'] : # no 'SET_ADD' and 'SET_DEL'
                # The write-set of a transaction consists of all oids to which the transaction writes; it excludes updates to set objects
                x['writeset'][update[1]]=update
                
        #如果所有要写的都是本地的
        print("[3.1] ---------获得写集合---------")
        print(x['writeset'])
        for oid in x['writeset'].keys():
            if get_oid_preferred_sites_id(oid) != siteID:
                print("[3.2] ---------慢提交--------------")
                com=threading.Thread(target=slowCommit,args=(x,))
                com.start()
                com.join()
                return 
        # 执行快提交
        print("[3.2] ---------快提交--------------")
        com=threading.Thread(target=fastCommit,args=(x,))
        com.start()
        com.join()

        print("==================================[事务 {}]======================================\n\n".format(x['outcome']))

        return 

    #-------------------------同步传播--------------------------#

    def propagate(self, x):
        # 给所有服务器传播同步信号
        print("╭────────────────────────────────[进程] 同步传播─────────────────────────────────────╮")
        print("│ ",x)
        print("│ --------------------------------------1. 传播--------------------------------------------------------")
        for server in servers:
            # 非本服务器
            if server[2]==False:
                serverUrl=server[3]
                print("│▲  同步到 站点 {} URL:{}".format(server[0],serverUrl))
                res=requests.post(serverUrl+"/propagate",json={"x":x,"id":siteID})
                print("|    ▷",res.text.replace("\n",""))
                if json.loads(res.text).get("status")=="ERROR":
                    print("╰─────────────────────────────────────────────────────────────────────────────────────╯")
                    return 
                #TODO:f+1 情况 和返回信息确认 先空缺
        # 标志
        print("| ♢事务现在 是 disaster-safe durable")
        x['mark']="disaster-safe durable"
        # 发送信号
        print("│ ---------------------------------------2. 灾难安全备份------------------------------------------------")
        for server in servers:
            # 非本服务器
            if not server[2]:
                serverUrl=server[3]
                print("│▲  同步到 站点 {} URL:{}".format(server[0],serverUrl))
                res=requests.post(serverUrl+"/ds_durable",json={"x":x,"id":siteID})
                print("|    ▷",res.text.replace("\n",""))
        # 标志
        print("| ♢事务现在 是 globally visible")
        x['mark'] = "globally visible"
        print("╰─────────────────────────────────────────────────────────────────────────────────────╯")

    #-------------------------history处理--------------------------#
    def history_VTS_visible(self, oid, VTS):
        '''返回oid的历史中，VTS可见的记录'''
        hiss=[]
        if oid in self.history.keys():
            for his in self.history[oid]:
                siteId,seq=his[1] # update中的<siteId, seqno>
                if VTS[siteId]>=seq:
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
    
