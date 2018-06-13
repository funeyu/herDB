# herDB

> herDB是一个基于hash索引实现的key/value小型nosql，可以内嵌于java程序里。herDB存储的文件分为.index文件与.data文件；
存储的key/value数据都是基于二进制文件存储。

> herDB's jar才不到40KB,实现简洁；支持并发操作，并且支持索引文件的扩容功能，get操作基本上一次磁盘随机读就能定位到数据。

## code samples
怎样创建herDB
``` java
  // "main.java.org.herDB.herdb"为目录名
  Configuration conf = Configuration.create("main.java.org.herDB.herdb");
  
  // 初始的情况下，slots的数组的大小
  conf.set(Configuration.SLOTS_CAPACITY, "65536");
  
  // 设置读写缓冲块的大小
  conf.set(Configuration.BUFFERED_BLOCK_SIZE, "8192");
  
  // 设置分段segmentIndex数组的大小
  conf.set(Configuration.SEGMENTS_SIZE, "8");
  
  // 设置key/value数据的最大长度
  conf.set(Configuration.ITEM_DATA_MAX_SIZE, "1024");
  
  // 参数“main.java.org.herDB.herdb”是目录名
  HerDB main.java.org.herDB.herdb = HerDB.create(conf, "main.java.org.herDB.herdb");
  
  main.java.org.herDB.herdb.put("china".getBytes(), "beijing".getBytes());
  main.java.org.herDB.herdb.put("wuhan".getBytes(), "donghu".getBytes());
  
  ......
  
  main.java.org.herDB.herdb.commit();
```
+ 每次创建一个herDB，在完成相关的操作后最后都要调用commit()方法
+ 创建配置文件的时候可以不用调用set方法；`Configuration conf = Configuration.create("main.java.org.herDB.herdb")`直接得到一个默认配置的文件；
如能估计到数据体量时，可以先将`Configuration.SLOTS_CAPACITY`的属性设置的大些，这样子就可以减少`resize()`的次数，提高性能。

打开一个已有的herdb数据库,可以再进行put get操作：
``` java
HerDB main.java.org.herDB.herdb = HerDB.open("main.java.org.herDB.herdb");

main.java.org.herDB.herdb.put("wuhan".getBytes(), "donghu".getBytes());
main.java.org.herDB.herdb.get("china".getBytes())
blablabla......

main.java.org.herDB.herdb.commit();
```

加快随机读的方法:
``` java
// 只读模式下将herdb文件映射到内存加快随机读
HerDB main.java.org.herDB.herdb = HerDB.openOnlyRead("main.java.org.herDB.herdb");
herDB.get("china".getBytes())
```

## 性能参数：
电脑性能：
+ 系统:OS XEI Capitan
+ 处理器: 2.6 GHz Intel Core i5
+ 硬盘: 1T的机械硬盘
添加数据的速度测试：
``` java
Configuration conf = Configuration.create("main.java.org.herDB.herdb");
conf.set(Configuration.BUFFERED_BLOCK_SIZE, "4096");
try {
    HerDB main.java.org.herDB.herdb = HerDB.create(conf, "main.java.org.herDB.herdb");
    long start = System.currentTimeMillis();
    for(int i = 0; i < 10000000; i ++){
    main.java.org.herDB.herdb.put(("key123"+ i).getBytes(), ("value案件司法就是发动机案说法jijaijdiajdifjaojfdiaodfijaosjdfoiajdfoiajfdi"
                        + "ijaijsdfoiajodfjaojfiaoijdfoiajfidajfidojaoijdfiojfiajsidfjiasjdfijaidsfjaiojfiajdfidajsdifjaisdfa"+i).getBytes());
            }
    System.out.println(System.currentTimeMillis() - start);
    main.java.org.herDB.herdb.commit();
  } catch (Exception e) {
    e.printStackTrace();
  }
```
一千万的数据追加在100s以内；索引占据的内存：100M左右；

随机读写的性能，针对上面的添加数据，在内存映射下只读模式测试：
``` java
try {
  HerDB main.java.org.herDB.herdb = HerDB.openOnlyRead("main.java.org.herDB.herdb");
  long start = System.currentTimeMillis();
  for(int i = 0; i < 10000000; i ++){
    main.java.org.herDB.herdb.get(("key123" + (int)(Math.random()* 10000000)).getBytes(), null);
  }
  System.out.println(System.currentTimeMillis() - start);
  main.java.org.herDB.herdb.commit();
  } catch (Exception e) {     
    e.printStackTrace();
  }
```
1000W次的随机读在10s以内；


## 实现细节：
[herDB的实现概要](http://funeyu.github.io/2016/04/18/herDB%E7%9A%84%E8%AE%BE%E8%AE%A1%E6%A6%82%E8%A6%81/)

## TO DO:
+ ~~添加lru热缓存实现~~；
+ 提供数据压缩接口
