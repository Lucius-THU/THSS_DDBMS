## 大数据系统软件 Lab1 实验报告

罗成  李蜀娟  郑舒文

#### 实验要求

实现 `models/cluster.go` 中的 `BuildTable` 与 `FragmentWrite` 函数以支持根据提供的表 schema 和分区规则 rules 新建分布式数据库并写入记录。



#### 实现方法

1. 在 `BuildTable` 函数中，对传入的 json 格式的 `rules`，先反序列化至如下数据结构的对象中：

    ```go
    type Rule struct {
    	Predicate
    	Column []string
    }
    ```

    然后对每个 Node 调用 `RPCCreateTable` 方法，此方法包裹着对 `CreateTable` 函数的调用，以便在不破坏原有创建表功能的基础上，对于新建的表添加分区规则等。

    **选择将分区规则分别写入对应 `Node` 所存储的表中，而不是在 `Cluster` 中存储，是基于提高分布式系统可靠性的考量。**如果 `Cluster` 出现异常，不会影响每个 `Node` 中的表正常读取自己的分区规则。`Table` 结构改造如下：

    ```go
    type Table struct {
    	schema, fullSchema *TableSchema
    	rowStore           RowStore
    	predicate          *Predicate
    }
    ```

2. 在 `FragmentWrite` 函数中，对于待插入的记录，遍历所有 `Node`，调用 `RPCCreateTable` 方法，其

    1. 检查 `Node` 中是否存在对应的表；
    2. 如果当前 `Node` 存在对应的表，则判断记录是否应当插入该表中。这部分主要是对每个 `ColumnSchema` 检查：
        1. 调用 `models/rule.go` 中的 `CheckType` 方法，判断 **待插入值的类型是否符合 `Schema` 指定的类型**；
        2. 调用 `models/rule.go` 中的 `Check` 方法，判断带插入值是否满足特定的分区规则。
    3. 如果通过了检查，就从待插入记录中提取出子表中所需 `schema` 所对应的值形成子记录并调用 `Insert` 方法以插入。



#### 实验结果

在 `models` 目录下执行

```shell
go test
```

代码顺利通过了现有的所有测例。



#### 补充内容

针对插入记录进行类型检查（类型不符合 `Schema` 定义不允许插入），此部分功能提供了额外的测例，见于 `models/extra_test.go`。（主要是在 `lab1_test.go` 的基础上对于每个 `Column` 增加了不符合类型定义的待插入记录。）