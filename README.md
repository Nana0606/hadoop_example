# Hadoop_Introduction
This repository includes some codes about hadoop basic operations.

# Custom_DataType
此文件夹下主要包含2个java文件，TextPair.java和TextPair2.java，都是自定义的数据类型。其中TextPair.java重写了write()和readFields()2个函数，TextPair2.java重写的函数较多

# HDFS_Operations
此文件夹下主要包含8个java文件和1个txt文件，Basic.java、CopyFile.java、CopyToHDFS.java、CreateFile.java、FindFileOnHDFS.java、GetLTime.java、HDFSMkdir.java、PutMerge.java和word.txt。其中word.txt是CopyToHDFS.java需要的本地文件。CopyFile.java和CopyToHDFS.java功能类似。PutMerge.java的实现思想是在文件上传的过程中，将文件合并，这样不需要先将所有文件上传到HDFS，再在HDFS上合并（效率较低且占用HDFS的空间）。GetLTime.java功能是获取文件修改时间。Basic.java中包含HDFS的基本操作，包括：list(), mkdir(), readFile(), ifExists(), putMerge(), renameFile(), addFile(), deleteFile(), getModificationTime(), getHostnames()

# Join
此文件夹下主要包括6个java文件和2个txt文件，CommonReduce.java、FirstComparator.java、JoinMain.java、KeyPartition.java、PreMapper.java、TextPair.java、action.txt和alipay.txt。主要功能是：action是商品和交易的匹配，alipay是商品和支付的匹配，求出交易和支付的相应记录（这个目前存在bug）

# MultiOutput
此文件夹下主要包含3个java文件和1个txt文件， MainJob.java、MultiOutPutMapper.java、MultiOutPutReducer.java和multioutput.txt，主要实现多文件输出，输出格式代码中有详细注释

# PartitionExample
此文件夹下主要包含3个java文件和1个txt文件， MapperClass.java、ReducerClass.java、TextPartition.java和partition.txt。主要功能是：给出若干行数据，每行由2或3或4个字符串组成，现将2个字符串、3个字符串和4个字符串的数据分别输出到不同文件，即2个数据的所有行放在一个文件......，partition.txt是输入文件

# Sort
此文件夹下主要包含7个java文件和1个txt文件， IntPaire.java、PartitionByText.java、SortMain.java、SortMapper.java、SortReducer.java、TextComparator.java、TextIntComparator.java和sort.txt，主要实现功能是：整合同一个key对应的不同value，将其显示成key value1,value2,value3的形式。其中value的是递增排序的

# Word Count
此文件夹下主要包含3个java文件和1个txt文件，MapperClass.java、ReducerClass.java、WordCounter.java和wordsCount.txt。主要功能是统计字符串出现的次数，txt文件是代码中用到的资源文件
