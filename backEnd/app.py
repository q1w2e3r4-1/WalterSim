# backend interface to communicate with the frontend
import threading
from .walter.server import Server
from flask import Flask, request


class Backend:
    def __init__(self, name):
        pass

    def create_app(self, name, db_server: Server):

        self.containers = {
            # uid : {posts:[],friends:[]}
        }

        app = Flask(name)

        # 刷新获得服务器的内容
        @app.route("/flush")
        def flush():
            myHistory, otherHistory = db_server.get_local_history()
            new_logs = db_server.get_new_logs()
            return {
                "myHistory":    myHistory,
                "otherHistory": otherHistory,
                "currentSeqNo": db_server.currentSeqNo,
                "CommittedVTS": db_server.committedVTS,
                "GotVTS":       db_server.gotVTS,
                "logs":         new_logs,
            }

        # 通过本接口处理一个事务，事务内容通过 POST 提交
        @app.route("/transaction", methods=['POST'])
        def transactions():
            print("\n\n==================================[事务]======================================")
            print("--------------------[1] 开始事务.--------------------")
            x = db_server.starTx()
            print(x)

            print("--------------------[2] 操作对象.--------------------")
            event_list = request.get_json()
            print(event_list)
            datas = {}
            try:
                for event in event_list:
                    event_type = event[0]
                    method = getattr(db_server, event_type, None)
                    if method is None:
                        continue
                    if "read" in event_type.lower():
                        ret = method(x, event[1])
                    else:
                        ret = method(x, event[1], event[2])
                    # read 返回 None 表示对象不存在，记录为 null 但继续执行
                    if ret is not None:
                        datas[event[1]] = ret
                    elif "read" in event_type.lower():
                        datas[event[1]] = None
                print(x)

                print("-------------------[3] 尝试提交.---------------------")
                db_server.commitTx(x)
                return {"status": x.outcome, "data": datas}
            except Exception as e:
                import traceback
                traceback.print_exc()
                x.outcome = "ABORTED"
                return {"status": "ABORTED", "error": str(e), "data": datas}, 500

        # history 接口
        @app.route("/history", methods=['POST'])
        def history():
            data = request.get_json()
            return {"data": db_server.history_VTS_visible(data["oid"], data["VTS"])}

        # 同步传播接收接口
        @app.route("/propagate", methods=['POST'])
        def do_propagate():
            data = request.get_json()
            x    = data['x']
            j    = data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{}传播请求".format(j))
            print("│   事务:", x)

            for i in range(len(x['startVTS'])):
                if i != db_server.siteID and x['startVTS'][i] > db_server.gotVTS[i]:
                    print("│   × 条件1不满足", x['startVTS'], db_server.gotVTS)
                    print("│<= {\"status\":\"ERROR\"}")
                    print("╰──────────────────────────────────────────────────────────────────────────╯")
                    return {"status": "ERROR"}, 200

            if db_server.gotVTS[j] != x['seqno'] - 1:
                print("│   × 条件2不满足", x['startVTS'], db_server.gotVTS)
                print("│<= {\"status\":\"ERROR\"}")
                print("╰──────────────────────────────────────────────────────────────────────────╯")
                return {"status": "ERROR"}, 200

            db_server.update(x['updates'], (j, x['seqno']))
            db_server.gotVTS[j] = x['seqno']
            print("│   ☑ 更新完成")
            print("│         ♢GotVTS: ", db_server.gotVTS)
            print("│         ♢History: ", db_server.history)
            print("│<= {\"status\":\"OK\"}")
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            return {"status": "OK", "tid": x['tid']}, 200

        # ds_durable 接收接口
        @app.route("/ds_durable", methods=['POST'])
        def do_ds_durable():
            data = request.get_json()
            x    = data['x']
            j    = data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{} 容灾请求".format(j))
            print("│   事务:", x)

            for i in range(len(x['startVTS'])):
                if x['startVTS'][i] > db_server.committedVTS[i]:
                    print("│   × 条件1不满足", x['startVTS'], db_server.committedVTS)
                    print("│<= {\"status\":\"ERROR\"}")
                    print("╰──────────────────────────────────────────────────────────────────────────╯")
                    return {"status": "ERROR"}, 200

            if db_server.committedVTS[j] != x['seqno'] - 1:
                print("│   × 条件2不满足", x['startVTS'], db_server.committedVTS)
                print("│<= {\"status\":\"ERROR\"}")
                print("╰──────────────────────────────────────────────────────────────────────────╯")
                return {"status": "ERROR"}, 200

            db_server.committedVTS[j] = x['seqno']
            print("│   ☑ 更新完成")
            print("│         ♢CommittedVTS: ", db_server.committedVTS)

            thread = threading.Thread(target=db_server.abort_unlock, args=(x['tid'],))
            thread.start()
            thread.join()
            print("│   ☑ 释放锁完成")
            print("│<= {\"status\":\"OK\"}")
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            return {"status": "OK", "tid": x['tid']}, 200

        # 慢投票接口
        @app.route("/prepare", methods=['POST'])
        def do_prepare():
            data     = request.get_json()
            tid      = data['tid']
            oids     = data['oids']
            startVTS = data['startVTS']
            j        = data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{} 慢提交投票请求".format(j))
            print("│   tid:{} \t oids:{}\t startVTS:{}".format(tid, oids, startVTS))

            retList = []
            thread  = threading.Thread(target=db_server.prepare_lock, args=(tid, oids, startVTS, retList))
            thread.start()
            thread.join()
            ret = retList[0]
            print("│▶ 投票结果 ", ret)
            print("│<= \"status\":\"{}\"".format("YES" if ret else "NO"))
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            return {"status": "YES" if ret else "NO"}

        # 慢投票解锁
        @app.route("/abort", methods=['POST'])
        def do_abort():
            data = request.get_json()
            tid  = data['tid']
            j    = data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{} 慢提交投票解锁请求".format(j))

            thread = threading.Thread(target=db_server.abort_unlock, args=(tid,))
            thread.start()
            thread.join()

            print("│▶ 完成")
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            return {"status": "OK"}

        #---------------------------用户数据相关接口---------------------------#

        @app.route("/user_data", methods=['POST'])
        def get_user_data():
            uid = request.args.get("uid")
            x = db_server.starTx()
            posts = db_server.readregularvisible(x, uid)
            members = db_server.setRead(x, uid)
            if members is not None:
                friends = [name for name, cnt in members.items() if cnt > 0]
            else:
                friends = []
            return {"status": "OK", "posts": posts, "friends": friends}, 200

        @app.route("/add_post", methods=['POST'])
        def add_post():
            """保留兼容接口；实际展示已改用 history 驱动，此接口可用于补充写入"""
            data    = request.get_json()
            uid     = data.get("uid", "")
            oid     = data.get("oid", "")
            content = data.get("content", "")
            if uid and oid and content:
                if uid not in self.containers:
                    self.containers[uid] = {"posts": {}, "friends": {}}
                self.containers[uid]["posts"][oid] = content
                return {"status": "OK"}, 200
            return {"status": "ERROR", "message": "uid/oid/content required"}, 400

        @app.route("/add_friend", methods=['POST'])
        def add_friend():
            data        = request.get_json()
            uid         = data.get("uid", "")
            setid       = data.get("setid", "")
            friend_name = data.get("friend_name", "")
            if uid and setid and friend_name:
                if uid not in self.containers:
                    self.containers[uid] = {"posts": {}, "friends": {}}
                fl = self.containers[uid]["friends"]
                fl.setdefault(setid, [])
                if friend_name not in fl[setid]:
                    fl[setid].append(friend_name)
                return {"status": "OK"}, 200
            return {"status": "ERROR", "message": "uid/setid/friend_name required"}, 400

        @app.route("/del_friend", methods=['POST'])
        def del_friend():
            data        = request.get_json()
            uid         = data.get("uid", "")
            setid       = data.get("setid", "")
            friend_name = data.get("friend_name", "")
            if uid and setid and friend_name:
                if uid not in self.containers:
                    return {"status": "OK"}, 200
                fl = self.containers[uid]["friends"]
                if setid in fl and friend_name in fl[setid]:
                    fl[setid].remove(friend_name)
                return {"status": "OK"}, 200
            return {"status": "ERROR", "message": "uid/setid/friend_name required"}, 400

        return app