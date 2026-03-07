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
- 四个预置事务
    - Alice发表说说 (A)
        1. Alice写数据到ssA01
        2. Alice读取ssA01
    - Bob发表说说并评论Alice (B)
        1. Bob写数据到ssB01
        2. Bob重写ssA01
        3. Bob读取ssA01
    - Alice添加Eva好友 (C)（可指定延时与下面冲突）
        1. Alice写flEva
        2. 读取flEva
    - Bob添加Eva好友 (D) （可指定延时）
        1. Bob写flEva
        2. 读取flEva

A相当于fast commit
B相当于slow commit
C和D不知何意味，首先他这个延时是html延时发送（而非不同site之间的延时），其次cset都不在writeset中，哪里来的冲突

#### TODO:
1. 添加更多demo事务，至少要把所有可能情况都覆盖到

2. 实验部分的复现，可以复用server00的逻辑，去掉一堆debug信息。
（要模拟出地域分布特征，通信延时较大）
