### WalterSim

> Implementing Walter系统 using Flask & Vue
>
> From paper 《Transactional storage for geo-replicated systems》

#### 后端演示图片
![](./imgs/site2.png)
#### 前端演示图片
![](./imgs/client2.png)

#### How to run this project
##### 1. start backend service
```cmd
cd .\backEnd\
python run_server.py
```
##### 2. start frontend Interface
Open `.html` file from `frontend` folder in the web browser.


#### 模拟说明 :partly_sunny:
- 分别模拟了两个站点 
    - site00 
    - site01
- 三个用户及首选站点
    - Alice  site00  ssA01 (regular object)
    - Bob   site01  ssB01 (regular object)
    - Eva    site01  flEva    (cset)
- 预置事务
    - Alice发表说说 (A: fast commit)
        1. Alice写数据到ssA01
        2. Alice读取ssA01
    - Bob评论Alice (B: slow commit)
        1. Bob写数据到ssB01
        2. Bob重写ssA01
        3. Bob读取ssA01
    - Alice添加Eva好友 (C)（可指定延时与下面冲突）
        1. Alice写flEva
        2. 读取flEva
    - Bob添加Eva好友 (D) （可指定延时）
        1. Bob写flEva
        2. 读取flEva
    (C+D: cset conflict solving)

    - eva读取Alice的说说（E 因果一致性+长分叉）
        1. alice（s00）发布说说 T1；
        2. eve（s01）在 T1 复制到 s01 前，在 s01 站点读取 alice 的说说（返回 “无数据”）；
        3. eve 在 s01 站点发布说说 T7；
        4. T1 异步复制到 s01 后，eve 再次读取，能看到 T1，但 T7 的提交顺序在 T1 之前（长分叉）；
        5. 所有用户最终按物理时间戳排序，看到 T1 在前、T7 在后。

python3 -m http.server 8080

#### 实验复现部分
在lab文件夹，详见其内部的README.md