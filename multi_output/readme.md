### 1、Requirements
multioutput主要是实现多输出的功能，功能需求如下：

**输入：**
```
1512,iphone5s,4英寸,指纹识别,A7处理器,64位,M7协处理器,低功耗
1512,iphone5,4英寸,A6处理器,IOS7
1513,iphone4s,3.5英寸,A5处理器,双核,经典
```

**输出：**
输出对应2个文件，一个是序号对应的文件，一个是值对应的文件

文件1：
```
1512
1512
1513
```

文件2：
```
iphone5s,4英寸,指纹识别,A7处理器,64位,M7协处理器,低功耗
iphone5,4英寸,A6处理器,IOS7
iphone4s,3.5英寸,A5处理器,双核,经典
```


### 2、Mapper
在mapper中，需要对输入加入一个key值，key便是从输入中提取的信号值，mapper之后的结果如下：
```
1512    iphone5s,4英寸,指纹识别,A7处理器,64位,M7协处理器,低功耗
1512    iphone5,4英寸,A6处理器,IOS7
1513    iphone4s,3.5英寸,A5处理器,双核,经典
```

### 3、Reducer
Reducer中需要设置多输出模式，引入一个MultipleOutputs并设置需要输出的内容即可。如reducer中内容所示：
```
multipleOutputs.write("KeySplit", NullWritable.get(), key.toString() + "/"); 
multipleOutputs.write("AllPart", NullWritable.get(), text); 
```



