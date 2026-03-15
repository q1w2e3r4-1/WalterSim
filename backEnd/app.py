# backend interface to communicate with the frontend
import requests
from .walter.server import Server
from flask import Flask, request

class Backend:
    def __init__(self, name):
        pass

    def create_app(self, host, port, name, db_server: Server):
        app = Flask(name)

        #刷新获得服务器的内容
        @app.route("/flush")
        def flush():
            myHistory,otherHistory=db_server.get_local_history()
            
            return {"myHistory":myHistory,"otherHistory":otherHistory,"currentSeqNo":db_server.currentSeqNo,"CommittedVTS":CommittedVTS,"GotVTS":GotVTS}


        #通过本接口处理一个事务 事务内容通过POST进行提交
        @app.route("/transaction",methods=['POST'])
        def transcations():
            #0.生成一个事务
            print("\n\n==================================[事务]======================================")
            #1.开始执行事务
            print("--------------------[1] 开始事务.--------------------")
            x=db_server.starTx()
            print(x)
            #2.执行事务中的具体动作
            print("--------------------[2] 操作对象.--------------------")
            event_list=request.get_json() #  获取事件的信息
            print(event_list)
            datas={}                      #  存储操作的结果数据
            for event in event_list:
                event_type=event[0] #动作名 也就是要执行的函数
                # 调用对应的函数直接运行
                ret=globals()[event_type](x,event[1],event[2]) if "read" not in event_type.lower() else globals()[event_type](x,event[1])
                if ret!=None:
                    datas[event[1]]=ret
            print(x)
            #3.尝试提交事务
            print("-------------------[3] 尝试提交.---------------------")
            db_server.commitTx(x)
            return {"status":x['outcome'],"data":datas}

        #history接口
        @app.route("/history",methods=['POST'])
        def history():
            data=request.get_json()
            return {"data":db_server.history_VTS_visible(data["oid"],data["VTS"])}

        #同步传播接收接口
        @app.route("/propagate",methods=['POST'])
        def do_propagate():
        
            data=request.get_json()
            x=data['x']
            j=data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{}传播请求".format(j))
            print("│   事务:",x)
            for i in range(len(x['startVTS'])):
                if i!=siteID and x['startVTS'][i]>GotVTS[i]:
                    print("│   × 条件1不满足",x['startVTS'],GotVTS)
                    print("│<= {\"status\":\"ERROR\"}")
                    print("╰──────────────────────────────────────────────────────────────────────────╯")
                    return {"status":"ERROR"},200
            if GotVTS[j]!=x['seqno']-1:
                print("│   × 条件2不满足",x['startVTS'],GotVTS)
                print("│<= {\"status\":\"ERROR\"}")
                print("╰──────────────────────────────────────────────────────────────────────────╯")
                return {"status":"ERROR"},200
            
            update(x['updates'],(j,x['seqno']))
            GotVTS[j]=x['seqno']
            print("│   ☑ 更新完成")
            print("│         ♢GotVTS: ",GotVTS)
            print("│         ♢History: ",History)
            # 返回确认信息
            print("│<= {\"status\":\"OK\"}")
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            return {"status":"OK","tid":x['tid']},200

        #ds_durable接收接口
        @app.route("/ds_durable",methods=['POST'])
        def do_ds_durable():
            data=request.get_json()
            x=data['x']
            j=data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{} 容灾请求".format(j))
            print("│   事务:",x)
            for i in range(len(x['startVTS'])):
                if x['startVTS'][i]>CommittedVTS[i]:
                    print("│   × 条件1不满足",x['startVTS'],CommittedVTS)
                    print("│<= {\"status\":\"ERROR\"}")
                    print("╰──────────────────────────────────────────────────────────────────────────╯")
                    return {"status":"ERROR"},200
            if CommittedVTS[j]!=x['seqno']-1:
                print("│   × 条件2不满足",x['startVTS'],CommittedVTS)
                print("│<= {\"status\":\"ERROR\"}")
                print("╰──────────────────────────────────────────────────────────────────────────╯")
                return {"status":"ERROR"},200
            #提交时间向量更新
            CommittedVTS[j]=x['seqno']
            print("│   ☑ 更新完成")
            print("│         ♢CommittedVTS: ",CommittedVTS)

            thread=threading.Thread(target=abort_unlock,args=(x['tid'],))
            thread.start()
            thread.join()
            print("│   ☑ 释放锁完成")
            # 返回确认信息
            print("│<= {\"status\":\"OK\"}")
            print("╰──────────────────────────────────────────────────────────────────────────╯")

            # 返回确认信息
            return {"status":"OK","tid":x['tid']},200

        #慢投票接口
        @app.route("/prepare",methods=['POST'])
        def do_prepare():
            data=request.get_json()
            tid     =data['tid']
            oids    =data['oids']
            startVTS=data['startVTS']
            j       =data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{} 慢提交投票请求".format(j))
            print("│   tid:{} \t oids:{}\t startVTS:{}".format(tid,oids,startVTS))

            # 判断并尝试开锁
            retList=[]
            thread=threading.Thread(target=prepare_lock,args=(tid,oids,startVTS,retList))
            thread.start()
            thread.join()
            ret=retList[0]
            print("│▶ 投票结果 ",ret)
            print("│<= \"status\":\"{}\"".format("YES" if ret else "NO"))
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            
            return {"status":"YES" if ret else "NO"}
        
        # 慢投票解锁  
        @app.route("/abort",methods=['POST'])
        def do_abort():
            data=request.get_json()
            tid     =data['tid']
            j       =data['id']
            print("\n╭──────────────────────────────────────────────────────────────────────────╮")
            print("│=> 接受到来自站点{} 慢提交投票解锁请求".format(j))  

            thread=threading.Thread(target=abort_unlock,args=(tid,))
            thread.start()
            thread.join()
            
            print("│▶ 完成")
            print("╰──────────────────────────────────────────────────────────────────────────╯")
            return {"status":"OK"}

        return app

    