package herdb;

import java.io.IOException;
import java.util.Arrays;

import index.IndexSegment;
import store.FSDirectory;

public final class HerDB {
    
    private Configuration conf;
    private IndexSegment[] segments;
    private FSDirectory fsd;
    
    private HerDB(Configuration conf, FSDirectory fsd, boolean isFirst) throws Exception{
        
        this.conf = conf;
        segments = new IndexSegment[conf.get(Configuration.SEGMENTS_SIZE)];
        
        this.fsd = fsd;
        
        for(int i = 0, length = segments.length; i < length; i ++) {
            segments[i] = IndexSegment.createIndex(fsd, "segment" + i, conf);
        }
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
        
        try {
            segments[ segmentFor(key) ].put(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // HerDB的get操作
    public byte[] get(byte[] key, byte[] value){
        
        byte [] results = null;
        return (results = segments[ segmentFor(key) ].get(key)) != null ? results : value;
    }
    
    // 获取分段IndexSegment的索引值
    private int segmentFor(byte[] key){
        
        return Arrays.hashCode(key) & conf.get(Configuration.SEGMENTS_SIZE) - 1;
    }
    
    public static void main(String[] args){
        
//        Configuration conf = Configuration.create("her");
//        conf.set(Configuration.BUFFERED_BLOCK_SIZE, "4096");
        
        try {
            HerDB herdb = HerDB.open("her");
//            HerDB herdb = HerDB.create(conf, "her");
//            for(int i = 0; i < 1000000; i ++){
//                herdb.put(("node" + i).getBytes(), ("values" + i).getBytes());
//            }
//            herdb.commit();
            
            System.out.println(new String(herdb.get("node105847".getBytes(), "no".getBytes())));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
