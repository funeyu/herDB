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
    
    // 用来记录文件块在文件中的具体的偏移
    private long offset;
    
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

    public void position(int position){
        
        this.position = position;
    }
    
    public int getPosition(){
        
        return this.position;
    }
    
    //获取当前position位置的int数字
    public int getInt(){
        
        position(this.position + 4);
        return NumberPacker.unpackInt(new byte[]{
            container[position],
            container[position + 1],
            container[position + 2],
            container[position + 3]
        });
    }
    
    // 获取该缓冲长度为span的字节数组
    public byte[] getBytes(int span){
        
        if(span == 0){
            return null;
        }
        
        int position = getPosition();
        position(position + span);
        return Arrays.copyOfRange(container, position, position+ span);
    }
    
    public int setLimit(int limit){
        
        this.limit = limit;
        return limit;
    }
    
    // 获取该读取文件块的有效大小
    public int getLimit(){
        
        return limit;
    }
    
    public BufferedBlock skip(int span){
        
        this.position += span;
        return this;
    }
    
    
    // 将data的字节数组包装到该BufferedBlock中
    public BufferedBlock wrap(byte[] data){
        
        if(data == null){
            return this;
        }
        
        for (int i = 0, length = data.length; i < length; i++) {
            container[i+limit] = data[i];
        }
        limit += data.length;
        offset += data.length;
        
        return this;
    }
    
    
    // 将data的有效的字节数组全部倒出
    public byte[] pour(){
        
        limit = 0;
        return Arrays.copyOfRange(container, 0, limit);
    }
    
    public long getOffset(){
        
        return offset;
    }
  
}
