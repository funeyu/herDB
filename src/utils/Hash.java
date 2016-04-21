package utils;

import java.util.Arrays;

public class Hash {
    private final static int segmentSize = 7;
    private Hash() {
    }

    public static int FNVHash1(byte[] data) {

        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (byte b : data)
            hash = (hash ^ b) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return Math.abs(hash);
    }
    
    /**
     * 根据key的byte数组　去获取相应的分段IndexSegments的数组下标
     * @param key
     * @return
     */
    public static int segmentNumberFor(byte[] key){
        
        return Arrays.hashCode(key) & segmentSize;
    }
}
