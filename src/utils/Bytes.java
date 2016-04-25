package utils;

import java.util.Arrays;

public final class Bytes {
    
    private Bytes(){}
    
    /**
     * 将数组the 与 other两个数组合并成一个数组并返回
     * @param the
     * @param other
     * @return
     */
    public final static byte[] join(byte[]the , byte[] other){
        
        if(the == null){
            return other;
        }
        if(other == null){
            return the;
        }
        
        byte[] result = new byte[ the.length + other.length ];
        
        for (int i = 0, length = the.length; i < length; i++){
            result[i] = the[i];
        }
        
        for (int i = 0, theLength = the.length, otherLength = other.length; i < otherLength; i++){
            result[theLength + i] = other[i];
        }
        
        return result;
    }
    
    /**
     * 将key 与 value打包成byte[]返回, 同时写入总长度<br>
     * <b>格式为:</b>datalength | keylength | key | value
     * 
     * @param key
     * @param value
     * @return
     */
    public final static byte[] wrapData(byte[] key, byte[] value) {

        // 先写入该wrapedData的长度
        byte[] datalength = NumberPacker.packInt(key.length + value.length + 4);
        // 写入key的长度
        byte[] keylength = NumberPacker.packInt(key.length);
        
        return join( join (join (datalength, keylength), key), value);
    }
    
    /**
     * 从一个byte[]整体里得出key的具体内容,<b>resultBytes格式: </b>
     * <pre>offset(5 bytes) + itemlength(4 bytes) + keylen(4 bytes) + key/value(n bytes)</pre>
     * @param resultBytes: Offset + itemData
     * @return key的bytes
     */
    public final static byte[] extractKey(byte[] resultBytes){
        int keyLength = NumberPacker.unpackInt(new byte[]{
                resultBytes[9],
                resultBytes[10],
                resultBytes[11],
                resultBytes[12]
        });
        
        byte[] key = new byte[keyLength];
        for(int i = 0; i < keyLength; i++ ){
            key[i] = resultBytes[13 + i];
        }
        
        return key;
    }
    
    /**
     * 从resultBytes得出offset数据
     * @param resultBytes
     * @return
     */
    public final static long extractOffset(byte[] resultBytes){
        
       return NumberPacker.unpackLong(new byte[]{
               resultBytes[0],
               resultBytes[1],
               resultBytes[2],
               resultBytes[3],
               resultBytes[4]
       });
    }
    
    /**
     * 从rsultsBytes得出itemData的数据
     * @param resultBytes
     * @return
     */
    public final static byte[] extractItemData(byte[] resultBytes){
        
        return Arrays.copyOfRange(resultBytes, 5, resultBytes.length);
    }
}
