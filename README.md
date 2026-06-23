# lab2 开发日志

## 2026-6-23

2026春哈工大（威海）分布式系统课程，应该是我大学三年以来为数不多的全勤的课程，并且在出勤率只有百分之20左右的情况下
我仍然坚持上课。功利地说，这门课有助于找工作，但我更愿意说，这门课更是一个兴趣爱好，同样，也是我大学最后一门文化课，
希望我的努力，能让我以全新的姿态去迎接大四，迎接毕业，迎接人生的下一个阶段。

特别感谢杜老师在课程中对我的指导，没有老师的指导，我可能仍然在早八的时间，昏睡在宿舍里，意淫着未来的美好生活，
却不付诸行动。

## 复盘

### 0. 总体理解

Raft 的目标是让多个节点在网络延迟、丢包、宕机、重启的情况下，对同一串日志达成一致。每条日志最终会被状态机按相同顺序执行，从而保证多个副本状态一致。

Raft 可以拆成几个部分：

1. Leader Election：选出唯一 leader。

2. Log Replication：leader 负责复制日志。

3. Safety：保证已提交日志不会被覆盖。

4. Persistence：崩溃恢复后不破坏一致性。

5. Snapshot：压缩已经应用的日志，避免日志无限增长。


---

### 1. 核心字段复盘

#### currentTerm

作用：

记录当前节点见过的最大任期，相当于 Raft 的逻辑时钟。

为什么需要：

用于识别过期 leader、过期 candidate 和过期 RPC。任何节点看到更大的 term，都必须更新自己的 term 并转为 follower。

不变量：

currentTerm 只能单调递增，不能回退。

---

#### votedFor

作用：

记录当前任期投票给了谁。

为什么需要：

保证一个节点在同一个 term 内最多投一票。

如果不持久化会怎样：

节点崩溃重启后可能在同一 term 给多个 candidate 投票，可能导致同一任期出现多个 leader。

---

#### log

作用：

保存客户端命令。Raft 通过复制日志实现复制状态机。

注意：

快照之后，log 的数组下标不再等于全局日志索引。

---

#### commitIndex

作用：

表示当前节点知道已经提交到的最大日志索引。

含义：

commitIndex 之前的日志已经可以被状态机执行。

---

#### lastApplied

作用：

表示当前节点已经应用到状态机的最大日志索引。

关系：

lastApplied <= commitIndex。

---

#### nextIndex

作用：

leader 为每个 follower 维护的字段，表示下一次准备发送给该 follower 的日志索引。

注意：

nextIndex 存的是全局日志索引，不是数组下标。

---

#### matchIndex

作用：

leader 确信某个 follower 已经复制成功的最大日志索引。

区别：

nextIndex 是猜测值，可能需要回退。  
matchIndex 是确认值，只在复制成功后推进。

---

#### lastIncludedIndex / lastIncludedTerm

作用：

表示快照覆盖到的最后一条日志的 index 和 term。

为什么需要 lastIncludedTerm：

快照截断日志后，AppendEntries 仍然需要使用 PrevLogIndex 和 PrevLogTerm 做一致性检查。lastIncludedIndex 和 lastIncludedTerm 相当于快照边界上的一条 dummy log。

---

### 2. 选举流程

#### 解决什么问题

Raft 需要在集群中选出一个 leader，由 leader 统一负责日志复制。

#### 正常流程

1. follower 长时间没有收到 leader 心跳。

2. follower 选举超时，变成 candidate。

3. candidate currentTerm 加一，投票给自己。

4. candidate 向其他节点发送 RequestVote。

5. 获得多数票后成为 leader。

6. leader 立即发送心跳，阻止其他节点发起选举。


#### RequestVote 判断

一个节点投票需要满足：

1. candidate 的 term 不小于自己。

2. 当前任期还没投票，或者已经投给这个 candidate。

3. candidate 的日志至少和自己一样新。


日志新旧判断：

先比较 lastLogTerm。  
term 更大的日志更新。  
term 相同再比较 lastLogIndex。
lastLogIndex 更大的日志更新

#### 关键不变量

同一任期最多只能有一个 leader。

原因：

每个节点同一任期最多投一票，而两个多数派一定有交集。
这也是为什么Raft将节点个数设置为奇数，如果是偶数，网络分区为节点数量相等的两个分区，都无法选举出leader

---

### 3. 日志复制复盘

#### 解决什么问题

leader 接收客户端命令后，需要把日志复制到多数节点，并保证 follower 的日志最终和 leader 一致。

#### AppendEntries 的作用

1. 心跳。

2. 日志复制。

3. 日志一致性检查。

4. 传递 leaderCommit。


#### 一致性检查

leader 发送日志时会带上：

PrevLogIndex  
PrevLogTerm


follower 必须检查自己在 PrevLogIndex 位置是否存在日志，并且 term 是否等于 PrevLogTerm。

如果匹配，说明前缀一致，可以追加新日志。

如果不匹配，说明日志冲突，需要拒绝，leader 回退 nextIndex。

在2D中，由于加入了 lastIncludedIndex 字段，即快照的最后一个index，还需要判断 args 的 prevLogIndex 和 lastIncludedIndex 的大小比较，如果 prevLogIndex 更小，说明过期
#### 冲突处理

如果 follower 发现已有日志和 leader 发来的日志在同一 index 但 term 不同，follower 要删除该位置以及之后的所有日志，然后追加 leader 的日志。

这里使用快速回退机制

如果follower的日志太短，就告诉leader，下一次你应该从我的最后来
如果term不匹配，那就找到该term的第一条索引，告诉leader，我这是第一条索引
leader判断自己有没有该term，有的话，那就从这个term的位置开始，如果没有，那就从第一条索引的位置开始


#### 关键不变量

如果两条日志在相同 index 处 term 相同，那么它们在该 index 之前的日志也相同。

---

### 4. commitIndex

#### 解决什么问题

日志复制到多数节点后，leader 需要判断哪些日志可以被提交。

#### leader 提交规则

leader 找到一个 N，满足：

1. N > commitIndex。

2. 多数节点的 matchIndex >= N。

3. log[N].Term == currentTerm。


满足后可以设置：

commitIndex = N。

#### 为什么只能直接提交当前任期日志

旧任期日志即使被复制到多数派，也不能直接提交。否则在某些 leader 切换场景下，旧任期日志仍可能被覆盖。

当前任期日志提交后，它之前的旧任期日志会被间接提交。

---

### 5. apply 流程复盘

#### 解决什么问题

commit 只表示日志已经安全，apply 才表示日志真正被状态机执行。

#### 正常流程

1. commitIndex 增大。

2. applier 被唤醒。

3. applier 把 lastApplied+1 到 commitIndex 的日志封装成 ApplyMsg。

4. 锁外发送到 applyCh。

5. 状态机按顺序执行。


#### 为什么不能持锁发送 applyCh

applyCh 可能阻塞。如果 Raft 持锁发送，RPC、Start、选举等逻辑都可能被阻塞，甚至死锁。

---

### 6. 持久化复盘

#### 需要持久化什么

1. currentTerm

2. votedFor

3. log

4. lastIncludedIndex

5. lastIncludedTerm

6. snapshot


#### 为什么 currentTerm 要持久化

防止节点崩溃后忘记自己见过的更高 term，从而参与旧任期逻辑。

#### 为什么 votedFor 要持久化

防止节点崩溃重启后在同一任期重复投票。

#### 为什么 log 要持久化

日志是已经参与共识的历史，丢失后可能破坏已提交日志。

#### 为什么 snapshot 要和 Raft 状态原子保存

如果先截断日志，再保存 snapshot，中间崩溃，就会出现日志没了、快照也没了的问题，状态无法恢复。

---

### 7. Snapshot 复盘

#### 解决什么问题

日志不能无限增长。Snapshot 把已经应用到状态机的日志压缩成当前状态，然后删除旧日志。

#### 什么时候可以快照

只能快照已经应用的日志。

也就是：

index <= lastApplied。

不能快照未提交或未应用日志，因为这些日志未来可能被覆盖。

#### 快照后日志结构

快照后，rf.log[0] 是 dummy entry，对应 lastIncludedIndex 和 lastIncludedTerm。

转换关系：

localIndex = globalIndex - lastIncludedIndex

globalIndex = lastIncludedIndex + localIndex

#### 关键不变量

1. lastIncludedIndex 表示快照覆盖到的最大全局索引。

2. lastIncludedTerm 表示该索引对应日志的任期。

3. rf.log[0].Term == lastIncludedTerm。

4. rf.log[1] 对应全局索引 lastIncludedIndex + 1。


---

### 8. InstallSnapshot 复盘

#### 解决什么问题

如果 follower 落后太多，它需要的日志已经被 leader 快照截断，leader 就不能继续用 AppendEntries 补日志，只能发送快照。

触发条件：

nextIndex[follower] <= lastIncludedIndex。

#### follower 收到快照后做什么

1. 检查 term。

2. 如果 leader term 更新，转为 follower。

3. 忽略旧快照。

4. 判断是否能保留快照之后的日志。

5. 更新 lastIncludedIndex 和 lastIncludedTerm。

6. 更新 commitIndex 和 lastApplied。

7. 原子持久化 Raft 状态和 snapshot。

8. 通过 applyCh 通知状态机安装快照。


#### 什么时候保留日志后缀

如果 follower 本地存在：

index == lastIncludedIndex  
term == lastIncludedTerm

说明快照边界一致，可以保留该日志之后的日志。

否则说明日志可能冲突，必须丢弃整个日志，只保留 dummy entry。

---


## 2026-6-17

### 八股复盘

## 2026-6-16
pass!!!!

![pass](./docs/images/img.png)

明天开始准备复盘！把这个README写好，重构就不用了， 本来就重构了一版，一共控制在了1000行以内自我感觉还是不错的

复盘内容准备如下：

- 详细的实现细节，踩坑记录，代码质量问题，等等
- 八股问题整理
## 2026-6-15
实现了全局日志

## 2026-6-14

本来想直接进行Lab2D实现的，但是觉得代码实在是太屎山了，于是重构了一版，相比上一版，感觉好看一下，也清晰一些

6月12 13 14 三天

## 2026-6-11

发现lab2D的实现，需要进行日志压缩，之前的lab2A和lab2B的实现没有考虑日志压缩，所以需要进行一些修改，来适应日志压缩的实现
就比如，lab2D的下标问题，我觉得之前的实现是基于日志不压缩的情况下的下标实现的


## 2026-6-10

lab2C较简单，并且把2B的内容给改了改。

## 2026-06-09

历时789三天，lab2B完成，依旧屎山这一块，依旧复盘等lab2全部完成再复盘这一块
## 2026-06-06

Lab2A 初步完成，使用
```bash
for i in {1..100}; do go test -run 2A || break; done
```
测试100次，全部通过

但是代码屎山，具体的实现细节和代码质量问题，以后再优化吧

具体的踩坑记录，后续再说吧

用时大概：八小时
## 2026-06-04

lab2 开始写代码

### 问题1:log[]如何实现
目前的想法是构建一个`logEntry`类，里面包含`term`和`command`两个属性


## 2026-06-01 -- 2026-06-03

阅读论文，了解Raft相关背景知识和算法细节 

个人认为像MIT助教说的一样，理解论文不能，但是细节实在是太多了
