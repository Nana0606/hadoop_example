### 1、Requirements
主要完成Join的功能。其输入输出如下：

输入：
```
action表：
product1"trade1
product2"trade2
product3"trade3

alipay表：
product1"pay1
product2"pay2
product2"pay4
product3"pay3
```

输出：
```
trade1	pay1
trade2	pay2
trade2	pay4
trade3	pay3
```

### 2、Mapper
在mappper中，将同一个productID对应的元素拥有同样的key送入到mapper，并记录是来自action表还是alipay表

### 3、Shuffle
拼接后传到reducer中

### 4、Reducer
获取内容，并解析action和alipay表中的内容。


**注：** 本join也可以从使用2个mapper+1个reducer的角度来考虑


