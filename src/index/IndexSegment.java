package index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import store.FSDirectory;
import store.InputOutData;
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
    private int compacity;
    // 标记attachedSlots里用到哪一个attachedSlot
    private int current;
    // 为文件的名称，这里索引文件与数据文件文件名称一致
    private String fileName;
    // todo: 放在configuration里
    // 索引文件的后缀
    private final static String INDEXSUFFIX = ".index";
    // 数据文件的后缀
    private final static String DATASUFFIX = ".data";
    // 扩容不能超过的最大容量
    private final static int MAXSIZE = 2 << 26;
    // index io操作的入口类
    private InputOutData fsIndex;
    // data io操作的入口类
    private InputOutData fsData;
    // 读取文件用到的文件块大小 默认先 64KB的大小
    // todo: 设置应该放在相应的配置文件里
    private int BufferedSize = 1 << 16;

    private IndexSegment(int compacity, int current, String fileName, byte[] bytes, InputOutData fs,
            InputOutData fsData) {
        this.compacity = compacity;
        this.current = current;
        this.fileName = fileName;
        this.bytes = bytes;
        this.fsIndex = fs;
        this.fsData = fsData;
    }

    public final static IndexSegment createIndex(FSDirectory fs, String fileName) throws Exception {

        boolean first = false;
        // index文件不存在则新建index文件
        if (!fs.isExsit(fileName + INDEXSUFFIX) && (first = true))
            fs.touchFile(fileName + INDEXSUFFIX);
        InputOutData fsIndex = fs.createDataStream(fileName + INDEXSUFFIX);

        if (!fs.isExsit(fileName + DATASUFFIX))
            fs.touchFile(fileName + DATASUFFIX);
        InputOutData fsData = fs.createDataStream(fileName + DATASUFFIX);

        if (first) {
            // todo:这里的设置应该放在configuration
            byte[] bytes = new byte[(1024 << 1) * Slot.slotSize + 4 + 4];
            return new IndexSegment(1024, 0, fileName, bytes, fsIndex, fsData);
        }

        byte[] bytes = fsIndex.readFully();
        return new IndexSegment(NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 0, 4)),
                NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 4, 8)), fileName, bytes, fsIndex, fsData);
    }

    // note: hashcode一定要保证不为0, 不然不能根据hc==0说明该slot空
    public void put(byte[] key, int hashcode, byte[] value) throws IOException {

        lock();
        try {
            byte[] slotBytes;      
            int hc;
            int index = Hash.FNVHash1(key) & (compacity - 1);
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
                    index = current + compacity;
                    
                    // attachedSlot full开始扩容
                    if (++current > compacity)
                        resize();
                    
                    // 设置上个slot 与本slot的关联
                    Slot.setAttachedSlot(oldindex, index, bytes);
                    
                    slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1)* Slot.slotSize + 8);
                } else {
                    index = Slot.getAttachedSlot(slotBytes);
                    slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
                }
            }
            
            if(isNew){
                // 新建一个slot
                byte[] newslot = Slot.generate(hashcode, fsData.maxOffSet(), 0);
                // 替换成新的slot
                Slot.replace(bytes, index * Slot.slotSize + 8, newslot);
            }
            
            // 写入key/value的数据到磁盘
            fsData.append(wrapData(key, value));

        } finally {
            unlock();
        }
    }

    /**
     * 将key 与 value打包成byte[]返回
     * 
     * @param key
     * @param value
     * @return
     */
    private byte[] wrapData(byte[] key, byte[] value) {

        byte[] wrappedData = new byte[key.length + value.length + 12];

        // 先写入该wrapedData的长度
        byte[] datalength = NumberPacker.packInt(key.length + value.length + 12);
        for(int i = 0; i < 4; i++) {
            wrappedData[i] = datalength[i];
        }
        
        // 写入key的长度
        byte[] keylength = NumberPacker.packInt(key.length);
        for (int i = 0; i < 4; i++) {
            wrappedData[i + 4] = keylength[i];
        }

        // 写入key的数据
        for (int i = 0, length = key.length; i < length; i++) {
            wrappedData[i + 8] = key[i];
        }

        // 写入value的长度
        byte[] valuelength = NumberPacker.packInt(value.length);
        for (int i = 0; i < 4; i++) {
            wrappedData[key.length + 8 + i] = valuelength[i];
        }

        // 写入value的数据
        for (int i = 0, length = value.length; i < length; i++) {
            wrappedData[key.length + 12 + i] = value[i];
        }

        return wrappedData;
    }

    // 根据key查找对应的value，没有则返回null
    public byte[] get(byte[] key) {

        int index = Hash.FNVHash1(key) & (compacity - 1);
        int hashcode = Arrays.hashCode(key);
        long offset;
        int keylen;
        int valuelen;
  
        byte[] slotBytes;
        
        lock();
        try {
            do {
                slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, 
                        (index + 1) * Slot.slotSize + 8);
                if(Slot.getHashCode(slotBytes) == hashcode) {
                    offset = Slot.getFileInfo(slotBytes);
                    try {
                        keylen = NumberPacker.unpackInt(fsData.position(offset+4).readSequentially(4));
                        // note: key的hashcode相等的情况下也有可能key不一致
                        if(Arrays.equals(fsData.readSequentially(keylen), key)) {
                            
                            valuelen = NumberPacker.unpackInt(fsData.readSequentially(4));
                            byte[] value = fsData.readSequentially(valuelen);
                            return value;
                        }
                    } catch (IOException e) {
                        
                        e.printStackTrace();
                        return null;
                    }
                }
            } while((index = Slot.getAttachedSlot(slotBytes)) != 0);
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
     * @throws IOException 
     */
    private void resize() throws IOException {
        
        BufferedBlock readingBlock = BufferedBlock.allocate(BufferedSize);
        BufferedBlock compressingBlock = BufferedBlock.allocate(BufferedSize);
        
        long offset = 0l;
        int distance = 0;
        boolean isDeleted = false;
        
        while(readingBlock.setLimit( fsData.readBlock(readingBlock.getBlock()) ) != -1) {
            
            offset += readingBlock.getLimit();
        }
    }
    
    /**
     * 从磁盘读取每个item，并在索引内存里判断其是否被删除
     * @return true: 被删除，无效的item； false：有效的item
     */
    private boolean isItemDeleted(){
        
        return false;
    }

    /**
     * 将内存的index文件flush到磁盘里
     */
    private void close() {
        // 写入索引的容量值
        Slot.replace(bytes, 0, NumberPacker.packInt(compacity));
        // 写入attachedSlot池中用到哪一个了
        Slot.replace(bytes, 4, NumberPacker.packInt(current));
        
        try {
            fsIndex.deleteFile().createNewFile().flush(bytes);
        } catch (IOException e) {
            
            e.printStackTrace();
        }
        bytes = null;
    }

    // 测试
    public static void main(String[] args) {
        byte[] key = null;
        try {
//            FSDirectorytory fsd = FSDirectory.open("her");
            FSDirectory fsd = FSDirectory.create("her", true);
            IndexSegment segment = IndexSegment.createIndex(fsd, "segment1");
            for (int i = 0; i < 1200; i++){
                key = ("key"+i).getBytes();
                byte[] value = ("value加手机发；" + i).getBytes();
                int hashcode = Arrays.hashCode(key);
                segment.put(key, hashcode, value);
            }
//            
            
            
//            key = "key2000".getBytes();
//            byte[] value = "value200000000000000000".getBytes();
//            int hashcode = Arrays.hashCode(key);
//            segment.put(key, hashcode, value);
            
            byte[] bytes =segment.get("key200".getBytes());
            System.out.println(new String(bytes));
            segment.close();
            
            
            
        } catch (Exception e) {

            e.printStackTrace();
        }
        
    }

}
