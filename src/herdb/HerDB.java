package herdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import cache.StorageCache;
import index.IndexSegment;
import serializer.SerializerImp;
import store.FSDirectory;

public final class HerDB {

    private Configuration conf;
    private IndexSegment[] segments;
    private FSDirectory fsd;
    private StorageCache cache;

    private SerializerImp serilization = SerializerImp.build();

    /**
     * HerDB的constructor,初始化IndexSegment数组
     *
     * @param conf    配置文件
     * @param fsd     FSDirectory实例
     * @param isFirst 是否第一次创建 是：true, 不是：false(即为打开的操作)
     * @throws Exception
     */
    private HerDB(Configuration conf, FSDirectory fsd, boolean isFirst) throws Exception {

        this.conf = conf;
        segments = new IndexSegment[conf.get(Configuration.SEGMENTS_SIZE)];

        this.fsd = fsd;

        for (int i = 0, length = segments.length; i < length; i++) {
            segments[i] = IndexSegment.createIndex(fsd, "segment" + i, conf);
        }

        this.fsd = fsd;

        this.cache = StorageCache.initCache(conf);
    }

    /**
     * 新建一个HerDB实例
     *
     * @param dirPath 数据库所在的目录
     * @return HerDB的实例
     * @throws Exception
     */
    public static HerDB create(Configuration conf, String dirPath) throws Exception {

        FSDirectory fsd = FSDirectory.create(dirPath, true);

        // 检查配置是否有问题
        conf.checkAndStore();
        // 默认情况下打开lru缓存
        conf.setOnOff(Configuration.IS_CACHE_ON, true);

        HerDB herDB = new HerDB(conf, fsd, true);
        return herDB;
    }

    /**
     * 根据dirPath打开一个HerDB的数据库
     *
     * @param dirPath
     * @return
     * @throws Exception
     */
    public static HerDB open(String dirPath) throws Exception {

        Configuration conf = Configuration.open(dirPath);
        //默认情况下打开lru缓存
        conf.setOnOff(Configuration.IS_CACHE_ON, true);
        FSDirectory fsd = FSDirectory.open(dirPath);

        return new HerDB(conf, fsd, false);
    }

    /**
     * 只打开一个现有的herDB数据内容，只进行读
     * 通过mmapfile 将文件映射到内存里来加快随机读
     *
     * @param dirPath HerDB的目录名称
     * @return
     * @throws Exception
     */
    public static HerDB openOnlyRead(String dirPath) throws Exception {

        Configuration conf = Configuration.open(dirPath);
        // 默认情况下打开lru缓存
        conf.setOnOff(Configuration.IS_ONLY_READ, true);
        FSDirectory fsd = FSDirectory.open(dirPath);

        return new HerDB(conf, fsd, false);
    }

    /**
     * 打开热缓存
     *
     * @return
     */
    public HerDB cacheOn() {

        conf.setOnOff(Configuration.IS_CACHE_ON, true);
        return this;
    }


    /**
     * 关闭热缓存
     *
     * @return
     */
    public HerDB cacheOff() {

        conf.setOnOff(Configuration.IS_CACHE_ON, false);
        return this;
    }

    /**
     * 每进行完一系列数据库操作，最后都要commit()，将内存索引文件写入磁盘中
     */
    public void commit() {

        for (int i = 0, length = segments.length; i < length; i++) {
            segments[i].commit();
        }
        // 删除herdb.lock的锁文件
        fsd.releaseDir();
    }

    public <T> void put(String key, T value) {

        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = serilization.serialize(value);
        putInternal(keyBytes, valueBytes);
    }

    public <T> T get(String key) {
        byte[] keyBytes = key.getBytes();
        byte[] resultBytes = getInternal(keyBytes, null);
        if (resultBytes == null)
            return (T) null;
        return serilization.deserialize(resultBytes);
    }

    public void putBytes(String key, byte[] value) {

        byte[] keyBytes = key.getBytes();
        putInternal(keyBytes, value);
    }

    public byte[] getBytes(String key) {

        byte[] keyBytes = key.getBytes();
        return getInternal(keyBytes, null);
    }
    
    // HerDB的put操作原生字节序列操作,所有的添加都要经过这一步
    private void putInternal(byte[] key, byte[] value) {

        // 先加入lru缓存中
        try {
            segments[segmentFor(key)].put(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过key的字节数组去查询相应的value字节数组，若没查到则返回一默认值
     *
     * @param key   查询的key
     * @param value 没查到时返回的默认值
     * @return
     */
    private byte[] getInternal(byte[] key, byte[] value) {

        byte[] results = null;

        //先查询lru缓存
        if ((results = (byte[]) cache.get(key)) != null) {
            return results;
        }

        // 添加到lru缓存
        if ((results = segments[segmentFor(key)].get(key)) != null) {
            cache.put(key, results);
            return results;
        }

        return value;
    }

    // 获取分段IndexSegment的索引值
    private int segmentFor(byte[] key) {

        return Arrays.hashCode(key) & conf.get(Configuration.SEGMENTS_SIZE) - 1;
    }


    // code example
    public static void main(String[] args) {

        Configuration conf = Configuration.create("her");
        conf.set(Configuration.BUFFERED_BLOCK_SIZE, "4096");

        try {
            HerDB herdb = HerDB.openOnlyRead("her");
//            HerDB herdb = HerDB.create(conf, "her");
            for (int i = 0; i < 1000; i++) {
//              herdb.put("key123"+ i, false);
                boolean result = (boolean) herdb.get("key123" + i);
                System.out.println(result);
            }
            herdb.commit();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
