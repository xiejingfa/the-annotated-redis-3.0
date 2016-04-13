# Redis-2.8.24 源码注释版
==============================

本项目是带有详细注释的 Redis 2.8.24 版源码。

在我的博客了有较为详细的源码分析文章，大家可以参考学习。

博客地址： http://blog.csdn.net/xiejingfa/article/category/6064201



希望这个项目能给您学习和了解 Redis 带来一些帮助。

最后，求star。


Enjoy Redis！



附：各源码文件的功能和相应的文章链接
------------------------------------------

| 文件           | 功能           | 
| ------------- | ------------- | 
| ``adlist.c`` 、 ``adlist.h``      | 双向链表list数据结构实现，[文章链接](http://blog.csdn.net/xiejingfa/article/details/50938028)。 |
| ``sds.c`` 、 ``sds.h``      | 字符串sds数据结构实现，[文章链接](http://blog.csdn.net/xiejingfa/article/details/50972592)。     |
| ``dict.c`` 、 ``dict.h``      | 字典dict数据结构实现，[文章链接](http://blog.csdn.net/xiejingfa/article/details/51018337)。     |
| ``ziplist.c`` 、 ``ziplist.h``      | 压缩列表ziplist数据结构实现，ziplist是为了节省列表空间而设计一种特殊编码方式，[文章链接](http://blog.csdn.net/xiejingfa/article/details/51072326)。     |
| ``zipmap.c`` 、 ``zipmap.h``      | 压缩字典zipmap数据结构实现，zipmap是为了节省哈希表空间而设计一种特殊编码方式，[文章链接](http://blog.csdn.net/xiejingfa/article/details/51111230)。     |
| ``intset.c`` 、 ``intset.h``      | 整数集合intset数据结构实现，[文章链接](http://blog.csdn.net/xiejingfa/article/details/51124203)。     | 
| ``object.c``      | Redis对象redisObject的实现，函数声明在redis.h文件中，[文章链接](http://blog.csdn.net/xiejingfa/article/details/51140041)。     |






