package herdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map.Entry;

public class Configuration {
    
    // 读写缓冲块的大小
    public final static String BUFFERED_BLOCK_SIZE = "buffered.block.size";
    // key/value 数据的最大长度
    public final static String ITEM_DATA_MAX_SIZE = "item.max.size";
    // 初始化segment的slot的大小
    public final static String SLOTS_CAPACITY = "slots.capacity";
    // 配置多少个分段
    public final static String SEGMENTS_SIZE = "segments.size";
    // 配置lru缓存的内存大小
    public final static String STORAGE_CACHE_SIZE = "storage.cache.size";
    // 存储配置信息
    public final static HashMap<String, String> conf = new HashMap<String, String>();
    
    // 配置临时的'开关项'变量
    public final static String IS_ONLY_READ = "isOnlyRead";
    public final static String IS_CACHE_ON ="isCacheOn";
    public final static HashMap<String, Boolean> tem = new HashMap<String, Boolean>();
    
    private final String dirPath;
    
    
    private Configuration(String dirPath){
        
        this.dirPath = dirPath;
        
        // 先设置默认的配置，这些配置项要写入文件磁盘中
        set(Configuration.BUFFERED_BLOCK_SIZE, "32768");
        set(Configuration.ITEM_DATA_MAX_SIZE, "1024");
        set(Configuration.SLOTS_CAPACITY, "32768");
        set(Configuration.SEGMENTS_SIZE, "8");
        // 1kb的lru缓存大小
        set(Configuration.STORAGE_CACHE_SIZE, "1048576");
        
        // 设置默认的临时开关项
        // 默认不打开热缓存开关
        tem.put(Configuration.IS_CACHE_ON, false);
        // 默认文件数据不是只读模式
        tem.put(Configuration.IS_ONLY_READ, false);
    }
    
    /**
     * 配置项key的修改
     * @param key
     * @param value
     * @return
     */
    public Configuration set(String key, String value){
        
        conf.put(key, value);
        return this;
    }
    
    // 缓存打开返回 true 否则 false
    public boolean isCacheOn(){
        
        return tem.get(Configuration.IS_CACHE_ON);
    }
    
    // 是否是只读模式
    public boolean isOnlyRead(){
        
        return tem.get(Configuration.IS_ONLY_READ);
    }
    
    /**
     * 开关项的配置设定
     * @param key 
     * @param onOrOff true:打开 false:关闭 
     */
    public void setOnOff(String key, boolean onOrOff){
        
        tem.put(key, onOrOff);
    }
    
    /**
     * 创建一个Configuration,默认的值
     * @param dirPath db所在的目录
     * @return Configuration的实例
     */
    public static Configuration create(String dirPath){
        
        Configuration conf = new Configuration(dirPath);
        return conf;
    }
    
    /**
     * 打开一个已存在的Configuration 读取其中的内容
     * @param dirPath configuration 所在的目录
     * @return Configuration的实例
     */
    public static Configuration open(String dirPath){
        
        Configuration conf = new Configuration(dirPath);
        try {
            conf.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return conf;
    }
    
    /**
     * 配置项检查，检查通过了就写入磁盘文件中，否则报异常
     */
    public void checkAndStore(){
        
        for( Entry<String, String> entry : conf.entrySet()) {
            try {
                Integer.parseInt(entry.getValue());
            } catch (NumberFormatException e){
                throw new IllegalArgumentException("can not parse the string:" + entry.getValue()
                + "to int type");
            }
        }
        
        // 要保证buffered.block.size > item.max.size * 2 
        if(Integer.parseInt(conf.get("buffered.block.size")) < Integer.parseInt(conf.get("item.max.size")) * 2) {
            throw new IllegalArgumentException("buffered.block.size must be greater than item.max.sieze * 2");
        }
        
        // 写入文件
        write();
    }
    
    /**根据key获取配置项的value
     * @param key
     * @return
     */
    public int get(String key){
        
        return Integer.parseInt(conf.get(key));
    }
    
    // 将配置文件写入磁盘
    private void write(){
        
        try {
            FileWriter fWriter = new FileWriter(dirPath 
                                             + "/herDB.conf", true);
            
            for (Entry<String, String> entry : conf.entrySet()) {
                fWriter.write( entry.getKey() + ":" + entry.getValue() );
                // 换行
                fWriter.write(System.getProperty("line.separator"));
            }
            
            fWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // 读取配置文件
    private void read() throws IOException {
        
        File file = new File(dirPath + "/herDB.conf");
        FileInputStream fis = new FileInputStream(file);
        BufferedReader fReader = new BufferedReader(new InputStreamReader(fis));
        
        String line ;
        while( (line = fReader.readLine()) != null ){
            String[] results = line.split(":");
            set(results[0], results[1]);
        }
        fReader.close();
    }

}
