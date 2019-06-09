### 1、Requirements
partition主要是实现自定义分区功能，根据每行字符串的长度进行分区，功能需求如下：

**输入：**
```
com 1 2
cat 2 3 4
dada 1
dudu 3 4
pha 5 4
```

**输出：**
输出文件对应多个partition，不同的字符串长度放在不同的分区中。

partition-1：
```
dada 1
```

partition-2：
```
com 1 2
dudu 3 4
pha 5 4

```
partition-3：
```
cat 2 3 4
```

### 2、Mapper
在mappper中，为了使得同一字符串长度的值进入到同一个key中，这里自定义key值，我们设置规则：若字符串的长度为2，则key=“short”；若字符串的长度为3，则key=“right”；若字符串的长度为4，则key=·“long”，mapper之后的结果如下：
```
short   [dada 1]
right   [com 1 2,dudu 3 4,pha 5 4]
long    [cat 2 3 4]
```

### 3、Reducer
这里reducer输出即可，不需要特殊处理

### 4、Partition
这里需要自定义partition，若key=“short”，则将其输入到``0 % numPartition``对应的partition中；若key=“right”，则将其输入到``1 % numPartition``对应的partition中；若key=“long”，则将其输入到``2 % numPartition``对应的partition中。



