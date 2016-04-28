package index;

import java.util.Arrays;
import utils.Bytes;
import utils.NumberPacker;

public class ReadingBufferedBlock extends BufferedBlock{
    
    // item被分割在上一block的数据
    private byte[] splitedBytes;
    private ReadingBufferedBlock(int capacity, int position) {
        super(capacity, position);
    }
    
    public static ReadingBufferedBlock allocate(int capacity){
        
        return new ReadingBufferedBlock(capacity, 0);
    }
    
    /**
     * 迭代取出block的item，每个item的<b>格式：</b>
     * <pre>
     * datalength | keylength | key | value<br>
     * <code> <b>datalength = </b>4 + key.length + value.length</code>
     * </pre>
     * @return 若无则返回 null,有完整item 则返回 offset(5字节) + item的字节数组
     */
    public byte[] nextItem(){
        
        byte[] itemBytes;
        //block的开头，要与之前的block的splitedBytes合并
        if(position == 0){
            if(splitedBytes != null){
                itemBytes = joinBytes(splitedBytes);
                return Bytes.join(NumberPacker.packLong( getOffset() - itemBytes.length ), itemBytes);
            }
        }
        
        int itemLen;
        long offSetData = getOffset();
        // 可以直接读取itemLen
        if(left() >= 4){
            itemLen = getInt();
            // 可以直接读取到完整的item数据
            if(left() >= itemLen){
                byte[] itemLenBytes = NumberPacker.packInt(itemLen);
                byte[] keyValueBytes = getBytes(itemLen);
                return Bytes.join(NumberPacker.packLong(offSetData), Bytes.join(itemLenBytes, keyValueBytes));
            } else {
                // 不能读取完整的item数据，返回null
                splitedBytes = Bytes.join(NumberPacker.packInt(itemLen), leftBytes());
                
                return null;
            }
        } else {
            // 不能完整获取itemLen
            splitedBytes = leftBytes();
            
            return null;
        }
    }
    
    // 根据splitedBytes 合并item
    private byte[] joinBytes(byte[] splitedBytes){
        
        byte[] thisPieces;
        if(splitedBytes.length >= 4){
            int itemLen = NumberPacker.unpackInt(new byte[]{
                    splitedBytes[0],
                    splitedBytes[1],
                    splitedBytes[2],
                    splitedBytes[3]
            });
            thisPieces = getBytes(itemLen - splitedBytes.length + 4);
            return Bytes.join(splitedBytes, thisPieces);
        }
        
        // 磁盘itemLen数据被分开在两个block，先组得出itemlen的数值
        int itemLen = NumberPacker.unpackInt(
                Bytes.join(splitedBytes, getBytes( 4 - splitedBytes.length)));
       
        return Bytes.join(NumberPacker.packInt(itemLen), getBytes(itemLen));
    }
    
    //获取当前position位置的int数字
    private int getInt(){
        
        int oldPosi = position;
        advance(4);
        return NumberPacker.unpackInt(new byte[]{
            container[oldPosi],
            container[oldPosi + 1],
            container[oldPosi + 2],
            container[oldPosi + 3]
        });
    }
    
    // 获取该缓冲长度为span的字节数组
    private byte[] getBytes(int span){
        
        if(span == 0){
            return null;
        }
        
        int oldPo = getPosition();
        advance(span);
        return Arrays.copyOfRange(container, oldPo, oldPo + span);
    }
    
    /**
     * 返回剩余的字节数组
     * @return
     */
    private byte[] leftBytes(){
        
        //　说明没有剩余，　item没被分在两个block里
        if(position == limit) {
            return null;
        }
        
        int temPo = position;
        advance(limit - position);
        return Arrays.copyOfRange(container, temPo, limit);
    }

}
