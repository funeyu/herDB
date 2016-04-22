package index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.crypto.dsig.CanonicalizationMethod;

import store.FSDirectory;
import store.InputOutData;
import utils.Bytes;
import utils.Hash;
import utils.NumberPacker;

/**
 * 分片索引文件的类
 * 
 * @author fuheu
 *
 */
public class IndexSegment extends ReentrantLock {
    // 索引在内存中的字节数组
    private byte[] bytes;
    // slotSize 的 1/2,这个值用来与做&hash计算得出index
    private int campacity;
    // 标记attachedSlots里用到哪一个attachedSlot
    private int current;
    // 为文件的名称，这里索引文件与数据文件文件名称一致
    private String fileName;
    // todo: 放在configuration里
    // 索引文件的后缀
    private final static String INDEXSUFFIX = ".index";
    // 数据文件的后缀
    private final static String DATASUFFIX = ".data";
    // 临时文件的后缀名
    private final static String TEMFILESUFFIX =  ".tep";
    // 扩容不能超过的最大容量
    private final static int MAXSIZE = 2 << 26;
    // index io操作的入口类
    private InputOutData fsIndex;
    // data io操作的入口类
    private InputOutData fsData;
    // FSDirectory的门面
    private FSDirectory fsd;
    // 读取文件用到的文件块大小 默认先 64KB的大小
    // todo: 设置应该放在相应的配置文件里
    private int BufferedSize = 1 << 10;

    private IndexSegment(int campacity, int current, String fileName, byte[] bytes, InputOutData fs,
            InputOutData fsData, FSDirectory fsd) {
        this.campacity = campacity;
        this.current = current;
        this.fileName = fileName;
        this.bytes = bytes;
        this.fsIndex = fs;
        this.fsData = fsData;
        this.fsd = fsd;
    }

    public final static IndexSegment createIndex(FSDirectory fsd, String fileName) throws Exception {

        boolean first = false;
        // index文件不存在则新建index文件
        if (!fsd.isExsit(fileName + INDEXSUFFIX) && (first = true))
            fsd.touchFile(fileName + INDEXSUFFIX);
        InputOutData fsIndex = fsd.createDataStream(fileName + INDEXSUFFIX);

        if (!fsd.isExsit(fileName + DATASUFFIX))
            fsd.touchFile(fileName + DATASUFFIX);
        InputOutData fsData = fsd.createDataStream(fileName + DATASUFFIX);

        if (first) {
            // todo:这里的设置应该放在configuration
            byte[] bytes = new byte[(1024 << 1) * Slot.slotSize + 4 + 4];
            return new IndexSegment(1024, 0, fileName, bytes, fsIndex, fsData, fsd);
        }

        byte[] bytes = fsIndex.readFully();
        return new IndexSegment(NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 0, 4)),
                                NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 4, 8)), 
                                fileName, bytes, fsIndex, fsData, fsd);
    }

    // note: hashcode一定要保证不为0, 不然不能根据hc==0说明该slot空
    public void put(byte[] key, int hashcode, byte[] value) throws IOException {

        lock();
        try {
            byte[] slotBytes;
            int hc;
            int index = Hash.FNVHash1(key) & (campacity - 1);
            int oldindex;
            byte[] oldkey;
            int attachedslot;
            boolean isNew = true;
            // 根据index获取该slot的byte[13]
            slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);

            while ((hc = Slot.getHashCode(slotBytes)) != 0) {
                if (hashcode == hc) {
                    oldkey = keyData(Slot.getFileInfo(slotBytes));

                    // 用新的索引数据替换旧的key数据，
                    if (Arrays.equals(oldkey, key)) {
                        isNew = false;
                        attachedslot = Slot.getAttachedSlot(slotBytes);
                        byte[] newslot = Slot.generate(hashcode, fsData.maxOffSet(), attachedslot);

                        Slot.replace(bytes, index * Slot.slotSize + 8, newslot);

                        break;
                    }
                }

                if ((attachedslot = Slot.getAttachedSlot(slotBytes)) == 0) {
                    oldindex = index;
                    index = current + campacity;

                    // attachedSlot full开始扩容
                    if (++current > campacity)
                        try {
                            resize();
                        } catch (Exception e) {
                            
                            e.printStackTrace();
                        }

                    // 设置上个slot 与本slot的关联
                    Slot.setAttachedSlot(oldindex, index, bytes);

                    slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
                } else {
                    index = Slot.getAttachedSlot(slotBytes);
                    slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
                }
            }

            if (isNew) {
                // 新建一个slot
                byte[] newslot = Slot.generate(hashcode, fsData.maxOffSet(), 0);
                // 替换成新的slot
                Slot.replace(bytes, index * Slot.slotSize + 8, newslot);
            }

            // 写入key/value的数据到磁盘
            fsData.append(Bytes.wrapData(key, value));

        } finally {
            unlock();
        }
    }

    // 根据key查找对应的value，没有则返回null
    public byte[] get(byte[] key) {

        int index = Hash.FNVHash1(key) & campacity - 1;
        int hashcode = Arrays.hashCode(key);
        long offset;
        int keylen;
        int valuelen;

        byte[] slotBytes;

        lock();
        try {
            do {
                slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
                if (Slot.getHashCode(slotBytes) == hashcode) {
                    offset = Slot.getFileInfo(slotBytes);
                    try {
                        keylen = NumberPacker.unpackInt(fsData.position(offset + 4).readSequentially(4));
                        
                        // note: key的hashcode相等的情况下也有可能key不一致
                        if (Arrays.equals(fsData.readSequentially(keylen), key)) {

                            valuelen = NumberPacker.unpackInt(fsData.readSequentially(4));
                            byte[] value = fsData.readSequentially(valuelen);
                            return value;
                        }
                    } catch (IOException e) {

                        e.printStackTrace();
                        return null;
                    }
                }
            } while ((index = Slot.getAttachedSlot(slotBytes)) != 0);
        } finally {
            unlock();
        }
        return null;
    }

    /**
     * 每个阶段的操作最终都要commit，如果在commit阶段，程序在执行扩容的话,就该等待程序执行完成 所以加lock()
     */
    public void commit() {
        lock();
        try {
            close();
        } finally {
            unlock();
        }
    }

    /**
     * 根据offset去获取key的bytes
     * 
     * @param offset
     * @return
     * @throws IOException
     */
    private byte[] keyData(long offset) throws IOException {

        // 获取key数据的长度
        int keyLength = NumberPacker.unpackInt(fsData.seek(offset, 4));

        return fsData.readSequentially(keyLength);
    }

    /**
     * 索引文件达到full即attachedSlots没有可以再利用 需要扩容
     * @throws Exception 
     */
    private void resize() throws Exception {

        if((campacity << 2) > MAXSIZE){
            throw new IllegalArgumentException(
                    "The comapacity:`" + campacity + "` is reaching its maxsize and can`t expend anymore");
        }
        
        BufferedBlock readingBlock = BufferedBlock.allocate(BufferedSize);
        BufferedBlock compressingBlock = BufferedBlock.allocate(BufferedSize);
        InputOutData temData = fsd.createDataStream(fileName + TEMFILESUFFIX);
        
        // 扩充后的内存索引数组
        byte[] newBytes = new byte[(campacity << 2) * Slot.slotSize + 8];
        // 标记磁盘的dataLength在上个block块的字节数组
        byte[] splitedByte = null;
        // 标记splitedByte在下一个block的长度
        int distance = 0;
        int itemLength = 0;
        // 标记每个key的长度
        int keyLength = 0;
        // 相应key的字节数组的数据
        byte[] keyBytes = null;
        // 相应value的字节数组的数据
        byte[] valueBytes = null;
        // 遍历文件内容， 记录文件偏移
        long oldOffset = 0l; 
        
        int test = 0;

        // fsData的文件的指针置于开头， 便于读
        fsData.jumpHeader();
        // 每读取一次的时候都要将块内存置头
        while (readingBlock.placeHeader()
                           .setLimit(fsData.readBlock(readingBlock.getBlock())) != -1) {
            if (splitedByte != null) {
                
                // 获取dataLength字节数组的另一半
                if(splitedByte.length < 4) {
                    byte[] other = readingBlock.getBytes(4 - splitedByte.length);
                    
                    //将数据的长度byte数组合并，并解析成int
                    itemLength = NumberPacker.unpackInt(Bytes.join(splitedByte, other));
                    
                    keyLength = readingBlock.getInt();
                    keyBytes = readingBlock.getBytes(keyLength);
                    
                    int valueLength = readingBlock.getInt();
                    valueBytes = readingBlock.getBytes(valueLength);
                    
                   if(isItemValid(keyBytes, oldOffset)){
                       putNative(keyBytes, oldOffset, newBytes);
                       compressingBlock.incOffset(itemLength);
                       temData.append(NumberPacker.packInt(keyLength))
                              .append(keyBytes)
                              .append(NumberPacker.packInt(valueLength))
                              .append(valueBytes);
  
                   }
                } else {
                    // splitedByte.length > 4
                    itemLength = NumberPacker.unpackInt(new byte[]{
                       splitedByte[0],
                       splitedByte[1],
                       splitedByte[2],
                       splitedByte[3]
                    });
                    // 剩余的bytes长度
                    int otherLength = itemLength - splitedByte.length;
                    // 将分开的字节数组合并成joinedBytes
                    byte[] joinedBytes = Bytes.join(splitedByte, readingBlock.getBytes(otherLength));
                    byte[] key = Bytes.extractKey(joinedBytes);
                    
                    if(isItemValid(key, oldOffset)){
                        putNative(key, oldOffset, newBytes);
                        compressingBlock.incOffset(itemLength);
                        temData.append(joinedBytes);
                    }
                }
            }
            
            while ( readingBlock.left() > 0) {
                
                test ++;
                if(test == 82){
                    System.out.println(test);
                }
                if( readingBlock.left() >= 4){
                    long offset = readingBlock.getOffset();
                    
                    itemLength = readingBlock.getInt();
                    
                    if(readingBlock.left() + 4 >= itemLength){
                        keyLength = readingBlock.getInt();
                        keyBytes = readingBlock.getBytes(keyLength);
                        
                        //　需要添加到compressingBlock
                        if(isItemValid(keyBytes, offset)){
                            // 此时的valueBytes的长度为: itemLength - 4[itemLength数字所占的字符] 
                            //                         - keyLength -4[keyLength数字的长度]
                            valueBytes = readingBlock.getBytes(itemLength - 4 - keyLength - 4);
                            
                            long newOffset = compressingBlock.getOffset();
                            putNative(keyBytes, newOffset, newBytes);//to check
                            
                            temData.append(Bytes.wrapData(keyBytes, valueBytes));
                        }
                        
                        continue;
                    } else {
                        // 记录此刻的文件偏移
                        oldOffset = readingBlock.getOffset();
                        // 将itemLength与剩余的byte打包成splitedByte
                        splitedByte = Bytes.join(NumberPacker.packInt(itemLength), 
                                                readingBlock.leftBytes());
                        break;
                    }
                } else {
                    // 记录此刻的文件偏移
                    oldOffset = readingBlock.getOffset();
                    // 剩下所有的字节数组, 此byte[] 的长度小于4
                    // 所以其itemLength的字节数组都被分割成在两个BufferedBlock
                    splitedByte = readingBlock.leftBytes();
                    break;
                }
            }
            
            temData.append(compressingBlock.pour());
        }
        
        // 删除旧的文件
        fsData.deleteFile();
        
        temData.reName(fileName + DATASUFFIX);
        
        fsData = temData;
        
        bytes = newBytes;
    }

    // 不加锁的put 只在扩容或者campact的时候调用,添加所以信息
    private void putNative(byte[] key, long offset, byte[]newBytes){
        
        byte[] slotBytes;
        int hashcode = Arrays.hashCode(key);
        int hc;
        int index = Hash.FNVHash1(key) & (campacity - 1);
        int oldindex;
        byte[] oldkey;
        int attachedslot;
        boolean isNew = true;
        // 根据index获取该slot的byte[13]
        slotBytes = Arrays.copyOfRange(newBytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
        System.out.println("offset；" + offset);

        while ((hc = Slot.getHashCode(slotBytes)) != 0) {
            
            if ((attachedslot = Slot.getAttachedSlot(slotBytes)) == 0) {
                oldindex = index;
                index = current + campacity;
                
                // 设置上个slot 与本slot的关联
                Slot.setAttachedSlot(oldindex, index, newBytes);

                slotBytes = Arrays.copyOfRange(newBytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
            } else {
                index = Slot.getAttachedSlot(slotBytes);
                slotBytes = Arrays.copyOfRange(newBytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
            }
        }

        if (isNew) {
            // 新建一个slot
            byte[] newslot = Slot.generate(hashcode, fsData.maxOffSet(), 0);
            // 替换成新的slot
            Slot.replace(newBytes, index * Slot.slotSize + 8, newslot);
        }
        
    }
    /**
     * 从磁盘读取每个item，并在索引内存里判断其是否有效
     * 
     * @return true: 无效的item； false：有效的item
     */
    private boolean isItemValid(byte[] key, long offset) {

        int index = Hash.FNVHash1(key) & campacity - 1;
        int hashcode = Arrays.hashCode(key);
        byte[] slotBytes;
        
        do {
            slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
            if (Slot.getHashCode(slotBytes) == hashcode) {
                long findOffset = Slot.getFileInfo(slotBytes);
                if(findOffset == offset){
                    return true;
                }
            }
        } while ((index = Slot.getAttachedSlot(slotBytes)) != 0);
        
        return false;
    }
    
    public void testChange(String name){
        
        this.fsData.reName(name);
    }

    /**
     * 将内存的index文件flush到磁盘里
     */
    private void close() {
        // 写入索引的容量值
        Slot.replace(bytes, 0, NumberPacker.packInt(campacity));
        // 写入attachedSlot池中用到哪一个了
        Slot.replace(bytes, 4, NumberPacker.packInt(current));

        try {
            fsIndex.deleteFile()
                   .createNewFile()
                   .flush(bytes);
        } catch (IOException e) {

            e.printStackTrace();
        }
        bytes = null;
    }

    // 测试
    public static void main(String[] args) {
        byte[] key = null;
        try {
//            FSDirectory fsd = FSDirectory.open("her");
            FSDirectory fsd = FSDirectory.create("her", true);
            IndexSegment segment = IndexSegment.createIndex(fsd, "segment1");
            for (int i = 0; i < 10000; i++) {
                key = ("key" + i).getBytes();
                byte[] value = ("value" + i).getBytes();
                int hashcode = Arrays.hashCode(key);
                segment.put(key, hashcode, value);
            }
//            
//            for (int i = 0; i < 10; i++) {
//                key = ("key" + i).getBytes();
//                byte[] value = ("new value" + i).getBytes();
//                int hashcode = Arrays.hashCode(key);
//                segment.put(key, hashcode, value);
//            }
             segment.testChange("sucess");
            //

            // key = "key2000".getBytes();
            // byte[] value = "value200000000000000000".getBytes();
            // int hashcode = Arrays.hashCode(key);
            // segment.put(key, hashcode, value);
            segment.close();

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

}
