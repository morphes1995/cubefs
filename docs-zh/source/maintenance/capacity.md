# 用户容量管理

## 扩容Volume空间

```bash
$ cfs-cli volume expand {volume name} {capacity / GB}
```
该接口用于增加volume容量空间。

::: tip 提示
DpReadOnlyWhenVolFull可以严格限制卷容量。

卷写满时不再继续写的配置方式：
(1)master
在创建卷时，将DpReadOnlyWhenVolFull参数设置为true;
若已创建的卷此值为false，可以update成trure。
当此值设置为true后，在卷写满时，master会把卷所有DP状态改为readonly。
(2)client
将client升级，并将client配置里“minWriteAbleDataPartitionCnt”参数设置为0。
:::

## Volume读写性能优化

可读写的dp数量越多，数据就会越分散，volume的读写性能会有响应提升。

CubeFS采取动态空间分配机制，创建volume之后，会为volume预分配一定的数据分区dp，当可读写的dp数量少于10个，会自动扩充dp数量。而如果希望手动提升可读写dp数量可以用以下命令：
```bash
$ cfs-cli volume create-dp {volume name} {number}
```

::: tip 提示
一个dp的默认大小为120GB，请根据volume实际使用量来创建dp，避免透支所有dp。
:::

## 回收Volume多余空间

```bash
$ cfs-cli volume shrink {volume name} {capacity in GB}
```

该接口用于减少volume容量空间, 会根据实际使用量计算，当设定值<已使用量的%120时操作会失败。

## 集群空间扩容

准备好新的dn和mn，启动配置文件配置现有master地址即可自动将新的节点添加到集群中。