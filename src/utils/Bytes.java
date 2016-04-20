package utils;

public final class Bytes {
    
    private Bytes(){}
    
    /**
     * 将数组the 与 other两个数组合并成一个数组并返回
     * @param the
     * @param other
     * @return
     */
    public final static byte[] join(byte[]the , byte[] other){
        
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
     * <b>格式为:</b>datalength | keylength | key | valuelength | value
     * 
     * @param key
     * @param value
     * @return
     */
    public final static byte[] wrapData(byte[] key, byte[] value) {

        // 先写入该wrapedData的长度
        byte[] datalength = NumberPacker.packInt(key.length + value.length + 12);
        // 写入key的长度
        byte[] keylength = NumberPacker.packInt(key.length);
        
        return join( join (join (datalength, keylength), key), value);
    }
    
    /**
     * 从一个byte[]整体里得出key的具体内容
     * @param campacity
     * @return
     */
    public final static byte[] extractKey(byte[] campacity){
        int keyLength = NumberPacker.unpackInt(new byte[]{
                campacity[4],
                campacity[5],
                campacity[6],
                campacity[7]
        });
        
        byte[] key = new byte[keyLength];
        for(int i = 0; i < keyLength; i++ ){
            key[i] = campacity[8 + i];
        }
        
        return key;
    }
}
