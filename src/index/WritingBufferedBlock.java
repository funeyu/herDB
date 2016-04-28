package index;

import java.util.Arrays;

public class WritingBufferedBlock extends BufferedBlock{

    private WritingBufferedBlock(int capacity, int position) {
        super(capacity, position);
    }
    
    public static WritingBufferedBlock allocate(int capacity){
        
        return new WritingBufferedBlock(capacity, 0);
    }
    
    /**
     * 判断写block是否有空间 容纳itemData
     * @param itemData key/value的数据
     * @return true 有空间， false 没空间
     */
    public boolean hasRoomFor(byte[] itemData){
        
        if(left() >= itemData.length){
            return true;
        }
        return false;
    }
    
    /**
     * 将文件缓冲读出，并将该文件缓冲置头
     * @return
     */
    public byte[] flush(){
        
        int oldPo = position;
        placeHeader();
        return Arrays.copyOfRange(container, 0, oldPo);
    }
  
    /**
     * 将data的字节数组包装到该缓冲文件中
     * @param data
     * @return
     */
    public BufferedBlock wrap(byte[] data){
        
        if(data == null){
            return this;
        }
        
        for (int i = 0, length = data.length; i < length; i++) {
            container[i+position] = data[i];
        }
        
        advance(data.length);
        return this;
    }

}
