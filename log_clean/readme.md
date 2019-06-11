### 1、Requirements
功能是从log日志中提取出ip、时间和网址，其输入输出如下：

输入：

```
110.75.173.48 - - [30/May/2013:23:59:58 +0800] "GET /thread-36410-1-9.html HTTP/1.1" 200 68629
220.181.89.186 - - [30/May/2013:23:59:59 +0800] "GET /forum.php?mod=attachment&aid=Mjg3fDgyN2E0M2UzfDEzNTA2Mjc3MzF8MHwxNTU5 HTTP/1.1" 200 -
112.122.34.89 - - [30/May/2013:23:59:59 +0800] "GET /forum.php?mod=ajax&action=forumchecknew&fid=91&time=1369929501&inajax=yes HTTP/1.1" 200 66
```


输出：
```
110.75.173.48	30/May/2013:23:59:58	thread-36410-1-9.html
220.181.89.186	30/May/2013:23:59:59	forum.php?mod=attachment&aid=Mjg3fDgyN2E0M2UzfDEzNTA2Mjc3MzF8MHwxNTU5
112.122.34.89	30/May/2013:23:59:59	forum.php?mod=ajax&action=forumchecknew&fid=91&time=1369929501&inajax=yes
```
其中，第一列为IP，第二列为时间，第三列为url

### 2、Mapper
在mappper中，需要对输入的每行数据进行解析，即Parser的功能，并对静态网站等进行过滤处理，最后包装成一个默认key，value是(Ip + "\t" + time + "\t" + url）的值，如下所示：
```
默认key	"110.75.173.48	30/May/2013:23:59:58	thread-36410-1-9.html"
默认key	"220.181.89.186	30/May/2013:23:59:59	forum.php?mod=attachment&aid=Mjg3fDgyN2E0M2UzfDEzNTA2Mjc3MzF8MHwxNTU5"
默认key	"112.122.34.89	30/May/2013:23:59:59	forum.php?mod=ajax&action=forumchecknew&fid=91&time=1369929501&inajax=yes"
```

### 3、Shuffle
之后Shuffule负责将结果传给Reducer

### 4、Reducer
将上述mapper中对应的value结果循环输出即可。


