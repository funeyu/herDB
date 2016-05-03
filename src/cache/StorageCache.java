package cache;

import java.util.LinkedHashMap;
import java.util.Map;

import herdb.Configuration;

public class StorageCache {
    
    private final LinkedHashMap cache;
    // 只记录key/value的长度，没有考虑LinkedHashMap每存一个数据占用的字节数
    private long cacheSize;
    private long currentSize;
    private final static float factor= 0.75F;
    
    
    /**
     *  用LinkedHashMap来做lru缓存，利用其removeEldestEntry函数
     * @param cacheSize 为lru缓存的最大key/value字节数
     */
    private StorageCache(int cacheSize) {
        
        cache = new LinkedHashMap(cacheSize, factor, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest){
                
                boolean isFull = currentSize > cacheSize;
                if (isFull) {
                    Object key = eldest.getKey();
                    Object value = eldest.getValue();
                    currentSize -= getSize(key) + getSize(value);
                }
                return isFull;
            }
        };
        this.cacheSize = cacheSize;
    }
    
    private StorageCache() {
        cache = null;
    }
    
    public static StorageCache initCache(Configuration conf) {
        
        if (conf.isCacheOn() && conf.get(Configuration.STORAGE_CACHE_SIZE) > 0) {
            return new StorageCache(conf.get(Configuration.STORAGE_CACHE_SIZE));
        }
        return new NoCache();
    }
    
    public Object get(Object key) {
        
        return cache.get(key);
    }
    
    public void put(Object key, Object value) {
        
        currentSize += getSize(key) + getSize(value);
        cache.put(key, value);
    }
    
    public boolean contains(Object key) {
        
        return cache.containsKey(key);
    }
    
    private int getSize(Object obj) {
        
        if (obj == null) {
            return 0;
        }
        return ((byte[])obj).length;
    }
    
    private static class NoCache extends StorageCache {
        
        NoCache() {
            
        }
        
        @Override
        public Object get(Object key) {
            return null;
        }
        
        @Override
        public void put(Object key, Object value) {
            
        }
    }

}
