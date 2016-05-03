package herdb;

import java.io.IOException;
import java.util.Arrays;

import cache.StorageCache;
import index.IndexSegment;
import store.FSDirectory;

public final class HerDB {
    
    private Configuration conf;
    private IndexSegment[] segments;
    private FSDirectory fsd;
    private StorageCache cache;
    
    /** 
     * HerDB的constructor,初始化IndexSegment数组
     * @param conf 配置文件
     * @param fsd FSDirectory实例
     * @param isFirst 是否第一次创建 是：true, 不是：false(即为打开的操作)
     * @throws Exception
     */
    private HerDB(Configuration conf, FSDirectory fsd, boolean isFirst) throws Exception{
        
        this.conf = conf;
        segments = new IndexSegment[conf.get(Configuration.SEGMENTS_SIZE)];
        
        this.fsd = fsd;
        
        for(int i = 0, length = segments.length; i < length; i ++) {
            segments[i] = IndexSegment.createIndex(fsd, "segment" + i, conf);
        }
        
        this.fsd = fsd;
        
        this.cache = StorageCache.initCache(conf);
    }
    
    /**
     * 新建一个HerDB实例
     * @param dirPath 数据库所在的目录
     * @return HerDB的实例
     * @throws Exception 
     */
    public static HerDB create(Configuration conf, String dirPath) throws Exception{
        
        FSDirectory fsd = FSDirectory.create(dirPath, true);
        
        // 检查配置是否有问题
        conf.checkAndStore();
        HerDB herDB = new HerDB(conf, fsd, true);
        return herDB;
    }
    
    /**
     * 根据dirPath打开一个HerDB的数据库
     * @param dirPath
     * @return
     * @throws Exception 
     */
    public static HerDB open(String dirPath) throws Exception{
        
        Configuration conf = Configuration.open(dirPath);
        FSDirectory fsd = FSDirectory.open(dirPath);
        
        return new HerDB(conf, fsd, false) ;
    }
    
    /**
     * 只打开一个现有的herDB数据内容，只进行读
     * 通过mmapfile 将文件映射到内存里来加快随机读
     * @param dirPath HerDB的目录名称
     * @return
     * @throws Exception 
     */
    public static HerDB openOnlyRead(String dirPath) throws Exception{
        
        Configuration conf = Configuration.open(dirPath);
        conf.setOnOff(Configuration.IS_ONLY_READ, true);
        FSDirectory fsd = FSDirectory.open(dirPath);

        return new HerDB(conf, fsd, false);
    }
    
    /**
     * 打开热缓存开关
     * @return
     */
    public HerDB cacheOn(){
        
        conf.setOnOff(Configuration.IS_CACHE_ON, true);
        return this;
    }
    
    /**
     * 每进行完一系列数据库操作，最后都要commit()，将内存索引文件写入磁盘中
     */
    public void commit(){
        
        for(int i = 0, length = segments.length; i < length; i ++) {
            segments[i].commit();
        }
        // 删除herdb.lock的锁文件
        fsd.releaseDir();
    }
    
    // HerDB的put操作
    public void put(byte[] key, byte[] value){
        
        // 先加入lru缓存中
        
        try {
            segments[ segmentFor(key) ].put(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 通过key的字节数组去查询相应的value字节数组，若没查到则返回一默认值
     * @param key 查询的key
     * @param value 没查到时返回的默认值
     * @return
     */
    public byte[] get(byte[] key, byte[] value){
        
        byte [] results = null;
        
        //先查询lru缓存
        if((results = (byte[])cache.get(key) )!= null) {
            return results;
        }
        
        // 添加到lru缓存
        if((results = segments[ segmentFor(key) ].get(key)) != null) {
            cache.put(key, results);
            return results;
        }
        
        return value;
    }
    
    // 获取分段IndexSegment的索引值
    private int segmentFor(byte[] key){
        
        return Arrays.hashCode(key) & conf.get(Configuration.SEGMENTS_SIZE) - 1;
    }
    
    public static void main(String[] args){
        
        Configuration conf = Configuration.create("her");
        conf.set(Configuration.BUFFERED_BLOCK_SIZE, "4096");
        
        
        try {
            HerDB herdb = HerDB.openOnlyRead("her");
//            HerDB herdb = HerDB.create(conf, "her");
            long start = System.currentTimeMillis();
            for(int i = 0; i < 1000000; i ++){
//                herdb.put(("key123"+ i).getBytes(), ("value案件司法就是发动机案说法jijaijdiajdifjaojfdiaodfijaosjdfoiajdfoiajfdi"
//                        + "ijaijsdfoiajodfjaojfiaoijdfoiajfidajfidojaoijdfiojfiajsidfjiasjdfijaidsfjaiojfiajdfidajsdifjaisdfa"+i).getBytes());
              herdb.get(("key123" + (int)(Math.random()* 10000000)).getBytes(), null);
            }
            System.out.println(System.currentTimeMillis() - start);
//            System.out.println(new String(herdb.get("node1231".getBytes(), "no".getBytes())));
            herdb.commit();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
