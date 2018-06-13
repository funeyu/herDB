package org.herDB.utils;

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
     * 计算key的hashcode，为了内存索引数据中的hashcode字段 == 0判断该slot是否为空；
     * 需要将key的hashcode全部转成不等于0的数据；
     *
     * @param key
     * @return 不为0的数据
     */
    public static int KeyHash(byte[] key) {

        int hashcode = Arrays.hashCode(key);
        return hashcode == 0 ? 1 : hashcode;

    }
}
