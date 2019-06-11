### 1、Requirements
提取出log日志中的月日，以及mac地址，其输入输出如下：

输入:
```
Apr 23 11:49:54 hostapd: wlan0: STA 14:7d:c5:9e:fb:84
Apr 23 11:49:52 hostapd: wlan0: STA 74:e5:0b:04:28:f2
```

输出：
```
Apr 23	14:7d:c5:9e:fb:84
Apr 23	74:e5:0b:04:28:f2
```

### 2、Mapper
这里正常解析数据即可，并将其封装成需要输出的字符串即可（注：这个示例没有reducer）。



