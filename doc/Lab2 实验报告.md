## 大数据系统软件Lab2 实验报告

李蜀鹃 罗成 郑舒文



#### 实验要求

实现`models/cluster.go`中的函数`Join`函数以支持根据给定表名实现自然连接， 无数据时返回长度为0的数组。



#### 实现方法

1. ##### 添加主键 

   对lab1实现的`BuildTable`进行改进，为每张表生成一个隐式主键id， 其生成方式使用uuid，类型为string， 放在每张表的第一列。并且在cluster中添加`tableName2id (map[string][]string)`, 建立表名到行主键的映射。

   

2. ##### Join

   实现流程为： 用两张表的Schema建立新表（Join结果）Schema -> 通过id获取完整行数据 -> 对行数据进行Join

   **2.1 新表Schema建立**

   （1）遍历节点，分别调用`Node.GetFullSchema`方法，获取两张表的完整Schema，如果节点返回的FullSchema不为空，停止该过程。

   （2）将获取到的两张表的列ColumnSchemas传给`createJoinSchema`方法，在这里，遍历两张表的schema，存储相同的列（连接键）索引。先将table1的schema加入新表schema，然后遍历table2的schema，借助存储的相同索引表，排除相同属性，将其他列存在新表schema后面。

   以下为该函数的执行示例：

   ```
   原表：table1 (a, b, c) table2 (b, d, e)
   执行createJoinSchema后：
   new_table(a,b,c,d,e)
   same_columns1(1) # 连接键在table1属性中的索引
   same_columns2(0) # 连接键在table2属性中的索引
   ```

   

   ##### 2.2 通过id获取完整行数据

   遍历`tableName2id`， 获取所有的行id， 然后调用`getLineByid`函数进行行数据的获取。

   `getLineByid`的实现思路为，遍历节点，根据表名取出id行，然后根据fullSchema补全一整行的数据，此时保证数据的顺序与建表一致，也可以处理不同分区有重复列的情况。

   

   ##### 2.3 对行数据进行Join

   得到两张表的整行数据后，遍历2.1获取到的相同列索引， 判断是否相同，若是，则将数据按照拼接schema的方式进行拼接（table1在前，table2与连接键不同的属性在后）。



#### 实验结果

由于增加了id， lab1的测试无法通过.

创建lab2_verticallyDivision_test.go, 包含4种测例：

①student垂直水平划分，courseRegistration水平划分

②student和courseRegistration均为空表

③student和courseRegistration垂直划分

④student和courseRegistration垂直划分，并且student的划分列有重复属性



首先执行

```shell
go get -u -v github.com/google/uuid
```

然后在 `models` 目录下执行

```shell
go test
```

通过lab2_test.go和lab2_verticallyDivision_test.go的所有样例。

