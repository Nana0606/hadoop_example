### 1、Requirements
word_frequencies和wordcount的功能一样，的主要功能是统计文档中每一个词出现的词频

**输入：**
```
Hadoop Spark
Hive Hadoop Hadoop
Database
```
**输出：**
```
Hadoop  3
Spark   1
Hive    1
Database    1
```

### 2、Mapper

#### 2.1、文档分割
主要将文档分割，比如对于如下内容：
```
Hadoop Spark
Hive Hadoop Hadoop
Database
```

我们首先需要将其进行分割，分割为“Hadoop”、“Spark”、“Hive”、“Hadoop”、“Hadoop”和“Database”，为了完成统计单词出现次数的功能，我们需要将这些单词作为key，设置“1”（出现的次数）作为value，格式如下：
```
key value
Hadoop  1
Spark   1
Hive    1
Hadoop  1
Hadoop  1
Database    1
```

#### 2.2、单词合并
在mappper中，key相同的value合并为value列表，并传送给reducer，内容如下：
```
Hadoop  [1, 1, 1]
Spark   [1]
Hive    [1]
Database    [1]
```

### 3、Combiner
这里加入一个Combiner，combiner的作用是本地reducer，比如，在mapper的若干节点中，如果其内容如下：
```
第一个节点：
Hadoop  [1, 1]
Spark   [1]

第二个节点：
Hadoop [1]
Hive    [1]
Database    [1]
```
则combiner的作用便是将其先本地reducer，然后再拼接传送给Reducer，combiner之后的结果为：
```
第一个节点：
Hadoop  [2]
Spark   [1]

第二个节点：
Hadoop [1]
Hive    [1]
Database    [1]
```

### 4、Partitioner（Shuffle的一部分）
将mapper的结果按照key进行分区。所以最终传送给Reducer的内容是（这步由Partition操作完成，将对应的key传给对应的reducer）：
```
Hadoop  [2,1]
Spark   [1]
Hive    [1]
Database    [1]
```
### 5、Reducer
Reducer接收来自mapper的结果，并对相同key的value进行加法操作，形成如下结果：
```
Hadoop  3
Spark   1
Hive    1
Database    1
```
将结果写入context即可。



