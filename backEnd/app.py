# backend interface to communicate with the frontend
import threading
from .walter.server import Server
from flask import Flask, request


class Backend:
    def __init__(self, name):
        pass

    def create_app(self, host, port, name, db_server: Server):
        app = Flask(name)

        # 刷新获得服务器的内容
        @app.route("/flush")
        def flush():
            myHistory, otherHistory = db_server.get_local_history()
            return {
                "myHistory":    myHistory,
                "otherHistory": otherHistory,
                "currentSeqNo": db_server.currentSeqNo,
                "CommittedVTS": db_server.committedVTS,
                "GotVTS":       db_server.gotVTS,
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
            for event in event_list:
                event_type = event[0]
                method = getattr(db_server, event_type, None)
                if method is None:
                    continue
                if "read" in event_type.lower():
                    ret = method(x, event[1])
                else:
                    ret = method(x, event[1], event[2])
                if ret is not None:
                    datas[event[1]] = ret
            print(x)

            print("-------------------[3] 尝试提交.---------------------")
            db_server.commitTx(x)
            return {"status": x.outcome, "data": datas}

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

        return app

