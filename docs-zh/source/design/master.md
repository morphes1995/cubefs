# 资源管理子系统

Master 负责异步的处理不同类型的任务， 比如 创建/删除/更新/比对副本是否一致等数据分片和元数据分片的操作，管理数据节点和元数据节点的存活状态，创建和维护卷信息。

Master 有多个节点，它们之间通过 raft 算法保证元数据一致性，并且把元数据持久化到 RocksDB。

## 基于利用率的分布策略

基于利用率的分布策略放置文件元数据和内容是 Master 最主要的特征，此分布策略能够更高效的利用集群资源。
数据分片和元数据分片的分布策略工作流程：
1. 创建卷的时候，Master 根据剩余磁盘/内存空间加权计算，选择权重最高的数据节点创建数据/元数据分片，写文件时，客户端随机选择数据分片和元数据分片。
2. 如果卷的大部分数据分片是只读，只有很少的数据分片是可读写的时候，master 会自动创建新的数据分片来分散写请求。

基于利用率的分布策略能带来两点额外的好处：

1. 当新节点加入时，不需要重新做数据均衡，避免了因为数据迁移带来的开销。
2. 因为使用统一的分布策略，显著降低了产生热点数据的可能性。

## 副本放置

Master 确保一个分片的多个副本都在不同的机器上。

## 拆分元数据分片

满足下面任意一个条件，元数据分片将会被拆分
1. 元数据节点内存使用率达到设置的阈值，比如总内存是 64GB，阈值是 0.75，如果元数据节点使用的内存达到 48GB，该节点上的所有元数据分片都将会被拆分。
2. 元数据分片占用的内存达到 16GB

::: warning 注意
只有元数据分片 ID 是卷所有元数据分片中 ID 最大的，才会真正被拆分。
:::

假设元数据分片 A 符合拆分条件，其 inode 范围是 `[0,正无穷)`，则拆分后 A 的范围为 `[0,A.MaxInodeID+step)`，新生成的 B 分片的范围是 `[A.MaxInodeID+step+1，正无穷)`，其中 step 是步长，默认是 2 的 24 方，MaxInodeID 是由元数据节点汇报。

## 异常处理

如果数据/元数据分片某个副本不可用 (硬盘失败、硬件错误等)，该副本上的数据最终会被迁移到新的副本上。