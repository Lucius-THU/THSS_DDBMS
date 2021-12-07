## 大数据系统软件Lab3 实验报告

郑舒文 李蜀鹃 罗成



#### 实验要求

在 Lab 1 和 Lab 2 的基础上修改 `models/cluster.go` 中的 `BuildTable`、`FragmentWrite` 和 `Join` 函数以支持冗余策略，实现同一分区在分布式节点上的多个副本和自然连接结果的去重。



#### 实现方法

1. 修改 `BuildTable` 函数，用 `strings.Split` 获取 rule 的 key 中的 1 个或多个分布式节点，并分别调用 `Node.RPCCreateTable` 建表。

   节点 `Node` 结构如下：

   ```go
   type Node struct {
   	Identifier string
   	// tableName -> table
   	TableMap map[string]*Table
   }
   ```

   由 `TableMap` 可以看出，节点以表名区分不同的表，而在冗余场景下，某个节点可能会被分配到同一张表的不同分区数据，在节点中创立分区时，

   ```go
   _, ok := n.TableMap[schema.TableName]; ok
   ```

   检测到已有该表名的分区存在，则返回 `table already exists` 错误。

   

   解决方式：采用别名方式唯一命名每个分区，第 i 条规则下的分区命名为 `tableName|i`。对 `Cluster` 修改如下：

   ```go
   type Cluster struct {
   	nodeIds      []string
   	tableName2id map[string][]string
   	// how many rules does this table have (How many copies of this table can a node have at most)
   	tableName2num map[string]int
   	network *labrpc.Network
   	Name string
   }
   ```

   新增 `tableName2num`，代表从表名到规则数量的映射，某节点上同一个表名的分区数量绝对不会超过规则数量。

2. 修改 `FragmentWrite` 函数，调用 `Node.RPCInsert` 插入数据行时，遍历规则数 i，传入的表名参数为`tableName|i`。

3. 修改 `Join` 函数，实质是修改 `getLineByid` 函数，调用 `Node.ScanLineData` 查询数据行时，遍历规则数 i，传入的表名参数为`tableName|i`。

   由于 `getLineByid` 函数已经考虑了列冗余，取出的数据严格符合原表 schema（详见 Lab 2 实验报告），故无需进一步修改。

#### 实验结果

在 `models` 目录下执行

```shell
go test
```

代码顺利通过了现有的所有测例。

