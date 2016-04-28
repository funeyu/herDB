# herDB

> herDB是一个基于hash索引实现的key/value小型nosql，可以内嵌于java程序里。herDB存储的文件分为.index文件与.data文件；
存储的key/value数据都是基于二进制文件存储。

> herDB's jar才不到30KB,实现简洁；支持并发操作，并且支持索引文件的扩容功能，get操作基本上一次磁盘随机读就能定位到数据。

## code samples
怎样创建herDB
``` java
  // "herdb"为目录名
  Configuration conf = Configuration.create("herdb");
  
  // 初始的情况下，slots的数组的大小
  conf.set(Configuration.SLOTS_CAPACITY, "65536");
  
  // 设置读写缓冲块的大小
  conf.set(Configuration.BUFFERED_BLOCK_SIZE, "8192");
  
  // 设置分段segmentIndex数组的大小
  conf.set(Configuration.SEGMENTS_SIZE, "16");
  
  // 设置key/value数据的最大长度
  conf.set(Configuration.ITEM_DATA_MAX_SIZE, "1024");
  
  // 参数“herdb”是目录名
  HerDB herdb = HerDB.create(conf, "herdb");
  
  herdb.put("china".getBytes(), "beijing".getBytes());
  herdb.put("wuhan".getBytes(), "donghu".getBytes());
  
  ......
  
  herdb.commit();
```
+ 每次创建一个herDB，在完成相关的操作后最后都要调用commit()方法
+ 创建配置文件的时候可以不用调用set方法；`Configuration conf = Configuration.create("herdb")`直接得到一个默认配置的文件；
如能估计到数据体量时，可以先将`Configuration.SLOTS_CAPACITY`的属性设置的大些，这样子就可以减少`resize()`的次数，提高性能。

打开一个已有的herdb数据库：
``` java
HerDB herdb = HerDB.open("herdb");

herdb.get();
herdb.put();
herdb.put();
blablabla......

herdb.commit();
```

## 实现细节：
[herDB的实现概要](http://funeyu.github.io/2016/04/18/herDB%E7%9A%84%E8%AE%BE%E8%AE%A1%E6%A6%82%E8%A6%81/)
