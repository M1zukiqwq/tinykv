# Raft 消息处理笔记

这份笔记记录当前 Project 2 的 Raft 实现在 `raft/raft.go` 中处理各种消息时，会具体修改哪些本地状态，以及会产生哪些输出消息。

## 核心心智模型

`Step` 是本地 Raft 状态机的统一事件入口。

每收到一条输入消息，Raft 通常做三件事：

1. 检查消息里的 term，如果对方 term 更新，就先退回 follower。
2. 修改本地状态，比如 `Term`、`Vote`、`State`、`Lead`、`RaftLog`、`Prs`。
3. 把需要发出去的消息追加到 `r.msgs`。

Raft 模块本身不真正发网络包。`sendAppend`、`sendHeartbeat`、投票回复等操作，本质都是把 `eraftpb.Message` 放进 `r.msgs`。测试网络或上层 RawNode/RaftStore 之后再负责真正投递这些消息。

## 通用 term 规则

`Step` 开头会先检查：

```go
if m.Term > r.Term {
    r.becomeFollower(m.Term, None)
}
```

意思是：如果一个节点看到更大的 term，就说明自己的任期过期了。无论它现在是 follower、candidate 还是 leader，都要更新 term，并退回 follower。

旧 term 的消息通常会在具体 handler 里被拒绝或忽略。

## 本地计时消息

### `MsgHup`

`MsgHup` 是本地选举事件。通常由 `tick()` 在 follower 或 candidate 达到随机 election timeout 后产生。

处理动作：

1. 如果自己已经是 leader，忽略。
2. 否则调用 `becomeCandidate()`。
3. `becomeCandidate()` 会递增 `Term`，给自己投票，清空 `Lead`，重置 election timeout。
4. 如果是单节点集群，直接调用 `becomeLeader()`。
5. 否则向其他 peer 发送 `MsgRequestVote`。

投票请求会携带候选人的最新日志信息：

```text
Index   = candidate last log index
LogTerm = candidate last log term
Term    = candidate current term
```

### `MsgBeat`

`MsgBeat` 是本地心跳事件。通常由 `tick()` 在 leader 达到 `heartbeatTimeout` 后产生。

处理动作：

1. 如果自己不是 leader，忽略。
2. 如果自己是 leader，对每个 follower 调用 `sendHeartbeat`。

心跳消息会带上 leader id、当前 term 和当前 commit index。

## 选举消息

### `MsgRequestVote`

`MsgRequestVote` 表示另一个节点请求本节点给它投票。

处理动作：

1. 如果请求的 term 过期，拒绝。
2. 否则只有同时满足两个条件才投票：
   - 本节点在这个 term 还没投票，或者已经投给同一个 candidate；
   - candidate 的日志至少和本节点一样新。
3. 如果投票成功，设置 `Vote = m.From`，并重置 election timeout。
4. 回复 `MsgRequestVoteResponse`。

日志新旧判断规则是：

```text
candidate last term > local last term
或者
candidate last term == local last term 且 candidate last index >= local last index
```

这个规则防止日志落后的节点成为 leader。

### `MsgRequestVoteResponse`

`MsgRequestVoteResponse` 是其他节点对本节点投票请求的回复。

处理动作：

1. 只有 candidate 处理这个消息。
2. 如果回复的 term 不是当前 term，忽略。
3. 记录 `r.votes[m.From] = !m.Reject`。
4. 如果同意票达到多数派，调用 `becomeLeader()`。
5. 如果拒绝票达到多数派，调用 `becomeFollower(r.Term, None)`。

成为 leader 时会做这些事：

1. 设置 `State = StateLeader`，`Lead = self`。
2. 初始化每个 peer 的 `Progress`。
3. 在当前 term 追加一条 noop entry。
4. 向 followers 发送 `MsgAppend`，复制这条 noop entry。

## Proposal 消息

### `MsgPropose`

`MsgPropose` 是本地应用层的写入请求。在 TinyKV 中，上层调用 `RawNode.Propose(data)`，最终会变成 `Step(MsgPropose)`。

处理动作：

1. 如果自己不是 leader，返回 `ErrProposalDropped`。
2. 把 proposed `[]*pb.Entry` 转成 `[]pb.Entry`。
3. 把这些 entries 追加到 leader 自己的日志。
4. `appendEntry` 会为每条 entry 填入 `Index` 和 `Term`。
5. 如果是单节点集群，立即 commit。
6. 否则向每个 follower 发送 `MsgAppend`。

这就是客户端写入命令进入 Raft 日志复制流程的入口。

## 日志复制消息

### `MsgAppend`

`MsgAppend` 是 leader 发给 follower 的 AppendEntries。它用于复制新日志，也用于把 leader 的 commit index 通知给 follower。

关键字段：

```text
Term    = leader current term
Index   = prevLogIndex
LogTerm = prevLogTerm
Entries = prevLogIndex 后面的新日志
Commit  = leader commit index
```

Follower 处理动作：

1. 如果 `m.Term < r.Term`，拒绝。
2. 否则调用 `becomeFollower(m.Term, m.From)`，承认 `m.From` 是 leader。
3. 检查本地日志在 `m.Index` 位置是否有 term 为 `m.LogTerm` 的 entry。
4. 如果 prev log 检查失败，回复 `MsgAppendResponse{Reject: true}`。
5. 如果 prev log 匹配，开始比较 leader 发来的 entries 和本地 entries。
6. 如果同 index 同 term 的 entry 已存在，保留。
7. 一旦发现同 index 但 term 冲突，删除本地这条 entry 以及后面的所有 entry。
8. 追加 leader 发来的剩余 entries。
9. 推进本地 `committed = min(m.Commit, lastNewIndex)`。
10. 回复 `MsgAppendResponse{Reject: false, Index: r.RaftLog.LastIndex()}`。

其中 prev log 检查是安全性的关键：follower 只有在确认前缀和 leader 匹配后，才会接受后续日志。

### `MsgAppendResponse`

`MsgAppendResponse` 是 follower 对 `MsgAppend` 的回复。

Leader 处理动作：

1. 只有当前 leader 且 response term 等于当前 term 时才处理。
2. 如果被拒绝：
   - 递减该 follower 的 `Progress.Next`；
   - 再次调用 `sendAppend`，从更早的位置重试。
3. 如果成功：
   - 更新该 follower 的 `Progress.Match = m.Index`；
   - 更新 `Progress.Next = Progress.Match + 1`。
4. 调用 `maybeCommit()`。
5. 如果 commit index 前进了，广播 `MsgAppend`，让 followers 学到新的 commit index。

`Match` 表示 leader 已确认该 follower 复制到的最高日志 index。

`Next` 表示 leader 下一次应该从哪个日志 index 开始发给这个 follower。

`maybeCommit()` 只有在两个条件都满足时才提交某个 index：

1. 多数派 peer 的 `Match >= index`；
2. 这个 index 上的 entry 属于 leader 当前 term。

第二个条件来自 Raft 的安全规则：leader 不能只靠多数派复制来直接提交旧 term 的 entry；旧 term 的 entry 会随着当前 term entry 被提交而间接提交。

## 心跳消息

### `MsgHeartbeat`

`MsgHeartbeat` 是 leader 发给 follower 的心跳。它用于确认 leader 身份，并防止 follower 发起新选举。

Follower 处理动作：

1. 如果 `m.Term < r.Term`，拒绝。
2. 否则调用 `becomeFollower(m.Term, m.From)`。
3. 回复 `MsgHeartbeatResponse`。

当前实现里，heartbeat 不直接推进 follower 的 commit index。leader 会在收到 heartbeat response 后调用 `sendAppend`，如果 follower 落后，就通过 AppendEntries 来同步日志和 commit。

这样可以避免 follower 在还没经过日志匹配检查前，仅凭 heartbeat 就把 commit 往前推。

### `MsgHeartbeatResponse`

`MsgHeartbeatResponse` 是 follower 对 heartbeat 的回复。

Leader 处理动作：

1. 只有当前 leader 且 response term 等于当前 term 时才处理。
2. 调用 `sendAppend(m.From)`。

这样 leader 可以利用 heartbeat response 顺手把落后的 follower 补齐。

## 角色切换

### `becomeFollower(term, lead)`

动作：

```text
Term = term
Lead = lead
State = StateFollower
votes = empty
heartbeatElapsed = 0
electionElapsed = 0
reset randomized election timeout
```

如果 `term > r.Term`，还会清空 `Vote`。

### `becomeCandidate()`

动作：

```text
State = StateCandidate
Term++
Lead = None
Vote = self
votes = {self: true}
heartbeatElapsed = 0
electionElapsed = 0
reset randomized election timeout
```

### `becomeLeader()`

动作：

```text
State = StateLeader
Lead = self
votes = empty
heartbeatElapsed = 0
electionElapsed = 0
initialize Progress for every peer
append noop entry in current term
send MsgAppend to followers
```

新 leader 追加 noop entry 很重要，因为 leader 需要当前 term 的 entry 来建立本任期下的 commit。

## Tick 行为

`tick()` 推进逻辑时间。

Follower 或 candidate：

```text
electionElapsed++
if electionElapsed >= randomizedElectionTimeout:
    electionElapsed = 0
    Step(MsgHup)
```

Leader：

```text
heartbeatElapsed++
if heartbeatElapsed >= heartbeatTimeout:
    heartbeatElapsed = 0
    Step(MsgBeat)
```

随机 election timeout 可以降低多个 follower 同时发起选举导致 split vote 的概率。
