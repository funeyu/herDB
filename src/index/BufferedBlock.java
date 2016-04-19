package index;

import java.util.Arrays;

import utils.NumberPacker;

public final class BufferedBlock {
    // 缓冲大小
    private final int capacity ;
    // 缓冲池操作指针指示的位置
    private int position;
    // 缓冲的具体二进制内容
    private final byte[] container;
    // 缓冲区域有效区的截止边
    private int limit;
    
    private BufferedBlock(int capacity, int position, byte[] container){
  
        this.capacity = capacity;
        this.position = position;
        this.container = container;
    }
    
    public static BufferedBlock allocate(int capacity){
        
        byte[] container = new byte[capacity];
        // 默认初始的情况 position:0
        return new BufferedBlock(capacity, 0, container);
    }
    
    public byte[] getBlock(){
        
        return container;
    }

    public BufferedBlock position(int position){
        this.position = position;
        return this;
    }
    
    public int getInt(){
        return NumberPacker.unpackInt(new byte[]{
            container[position],
            container[position + 1],
            container[position + 2],
            container[position + 3]
        });
    }
    
    // 获取该缓冲start 到 end的字节数组
    public byte[] getBytes(int start, int end){

        return Arrays.copyOfRange(container, start, end);
    }
    
    public int setLimit(int limit){
        
        this.limit = limit;
        return limit;
    }
    
    // 获取该读取文件块的有效大小
    public int getLimit(){
        
        return limit;
    }
    
    public void wrap(byte[] data){
        
        for (int i = 0, length = data.length; i < length; i++) {
            container[i+limit] = data[i];
        }
        limit += data.length;
    }
}
