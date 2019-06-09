sort的主要功能是对文档中相同字符串对应的value按序拼接，其主要思想如下：

### Mapper

输入文件格式如下：
```
str1	2
str2	5
str1	5
str1	3
str2	7
str2	1
```

在mappper中，key相同的value合并为value列表，并传送给reducer，内容如下：
```
str1  [2, 5, 3]
str2   [5, 7, 1]
```

### Reducer
Reducer接收来自mapper的结果，并对相同key的value进行拼接，在拼接之前我们先对value列表进行排序，形成如下结果：
```
str1    2,3,5
str2    1,5,7
```
将结果写入context即可。

### Sort
上述提到我们需要对value列表首先进行排序，接着才可以对其在reducer中进行拼接。除此之外，输出时候的key也需要按序排列，所以TextComparator便是基于key的排序，TextIntCompator便是基于value的排序。这两个比较器在main中都需要进行设置。如下（可参见SortMain.java中的代码）
```
job.setSortComparatorClass(TextIntComparator.class);
job.setGroupingComparatorClass(TextComparator.class);
```


