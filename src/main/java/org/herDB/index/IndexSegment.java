package org.herDB.index;

import com.sun.istack.internal.NotNull;
import org.herDB.herdb.Configuration;
import org.herDB.store.FSDirectory;
import org.herDB.store.InputOutData;
import org.herDB.utils.Bytes;
import org.herDB.utils.Hash;
import org.herDB.utils.NumberPacker;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;



/**
 * 分片索引文件的类
 *
 * @author funeyu
 */
@SuppressWarnings("serial")
public class IndexSegment extends ReentrantLock {

    private IndexMemoryByte indexMemoryByte;
    // 为文件的名称，这里索引文件与数据文件文件名称一致
    private String fileName;
    // todo: 放在configuration里
    // 索引文件的后缀
    private final static String INDEXSUFFIX = ".index";
    // 数据文件的后缀
    private final static String DATASUFFIX = ".data";
    // 临时文件的后缀名
    private final static String TEMFILESUFFIX = ".tep";
    // 扩容不能超过的最大容量
    private final static int MAXSIZE = 2 << 26;
    // main.java.org.herDB.index io操作的入口类
    private InputOutData fsIndex;
    // data io操作的入口类
    private InputOutData fsData;
    // FSDirectory的门面
    private FSDirectory fsd;
    // 读取文件用到的文件块大小 默认先 64KB的大小
    private int BufferedSize = 1 << 10;

    /**
     * @param campacity
     * @param current
     * @param fileName
     * @param fs
     * @param fsData
     * @param fsd
     * @param isFirst
     */
    private IndexSegment(int campacity, int current, String fileName, InputOutData fs,
                         InputOutData fsData, FSDirectory fsd, boolean isFirst) {
        this.indexMemoryByte = isFirst ? IndexMemoryByte.init(campacity, current) :
                IndexMemoryByte.open(fsd.readIndexFully(fileName + INDEXSUFFIX));
        this.fileName = fileName;
        this.fsIndex = fs;
        this.fsData = fsData;
        this.fsd = fsd;
    }

    /**
     * 根据FSDirectory创建或者打开IndexSegment,首次则创建IndexSegment,
     * 非首次则打开IndexSegment
     *
     * @param fsd
     * @param fileName
     * @return
     * @throws Exception
     */
    public final static IndexSegment createIndex(FSDirectory fsd, String fileName, Configuration conf)
            throws Exception {

        boolean first = false;

        // index文件不存在则新建index文件
        if (!fsd.isExsit(fileName + INDEXSUFFIX) && (first = true))
            fsd.touchFile(fileName + INDEXSUFFIX);
        InputOutData fsIndex = fsd.createDataStream(fileName + INDEXSUFFIX, false);

        if (!fsd.isExsit(fileName + DATASUFFIX))
            fsd.touchFile(fileName + DATASUFFIX);
        InputOutData fsData = fsd.createDataStream(fileName + DATASUFFIX, conf.isOnlyRead());

        if (first) {
            return new IndexSegment(conf.get(Configuration.SLOTS_CAPACITY), 0, fileName, fsIndex,
                    fsData, fsd, true);
        }

        byte[] bytes = fsIndex.readFully();
        return new IndexSegment(NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 0, 4)),
                NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 4, 8)),
                fileName, fsIndex, fsData, fsd, false);
    }


    /**
     * put操作的实现；
     * <ul>
     * <li>更新内存索引的数据</li>
     * <li>往磁盘里写入文件</li>
     * </ul>
     *
     * @param key
     * @param value
     * @throws IOException
     */
    public void put(byte[] key, byte[] value) throws IOException {

        // key经FVHash1的hash函数得出hash值
        int index = Hash.FNVHash1(key) & indexMemoryByte.capacity() - 1;
        // key利用系统函数hashCode的出hashcode
        int hashcode = Hash.KeyHash(key);
        // 标记是否在attachedSlot
        boolean isInAttachedSlot = false;

        lock();
        try {
            int hc;

            while ((hc = indexMemoryByte.getHashCode(index)) != 0) {
                isInAttachedSlot = true;

                // put的key可能与之前已加入的数据相等
                if (hashcode == hc) {
                    // 获取index相应的key磁盘数据
                    byte[] oldkey = keyData(indexMemoryByte.getFilePosition(index));

                    // 用新的索引数据替换旧的key数据，
                    if (Arrays.equals(oldkey, key)) {
                        int attachedslot = indexMemoryByte.getAttachedSlot(index);
                        indexMemoryByte.replaceSlot(hashcode, fsData.maxOffSet(), attachedslot, index);

                        break;
                    }
                }

                if (indexMemoryByte.getAttachedSlot(index) == 0) {
                    int oldindex = index;
                    try {
                        index = indexMemoryByte.nextCurrent();
                    } catch (IndexOutofRangeException iore) {
                        // 如果catch indexoutofrangeException 就开始扩容
                        try {
                            resize();
                        } catch (Exception e) {

                            e.printStackTrace();
                        }
                        // 扩容后重新计算index
                        index = Hash.FNVHash1(key) & (indexMemoryByte.capacity() - 1);
                        isInAttachedSlot = false;

                        continue;
                    }
                    // 设置上个slot 与本slot的关联
                    indexMemoryByte.setAttachedSlot(oldindex, index);
                    // 更新slot的数据
                    indexMemoryByte.replaceSlot(hashcode, fsData.maxOffSet(), 0, index);
                    break;
                } else {
                    index = indexMemoryByte.getAttachedSlot(index);
                }

            }
            // 没有发生hash碰撞的情况
            if (!isInAttachedSlot) {
                indexMemoryByte.replaceSlot(hashcode, fsData.maxOffSet(), 0, index);
            }
            // 写入key/value的数据到磁盘
            fsData.append(Bytes.wrapData(key, value));
        } finally {
            unlock();
        }
    }

    /**
     * 根据 key 获取 value的字节数组；
     *
     * @param key
     * @return byte[] or null
     */
    public byte[] get(byte[] key) {

        int index = Hash.FNVHash1(key) & indexMemoryByte.capacity() - 1;
        int hashcode = Hash.KeyHash(key);

        lock();
        try {
            do {
                if (indexMemoryByte.getHashCode(index) == hashcode) {
                    long filepo = indexMemoryByte.getFilePosition(index);
                    try {
                        // key/value磁盘数据格式： 
                        // datalength(4 bytes) + keylength(4 bytes) + key(raw data) + value(raw data) 
                        // datalength的大小：4 + key的字节长度 + value的字节长度
                        long start = System.currentTimeMillis();
                        int datalen = NumberPacker.unpackInt(
                                fsData.position(filepo)
                                        .readSequentially(4));
                        int keylen = NumberPacker.unpackInt(
                                fsData.readSequentially(4));
                        int valuelen = datalen - keylen - 4;
                        long end = System.currentTimeMillis();
                        long during = end - start;
                        // note: key的hashcode相等的情况下也有可能key不一致
                        if (Arrays.equals(fsData.readSequentially(keylen), key)) {
                            byte[] value = fsData.readSequentially(valuelen);
                            return value;
                        }
                    } catch (IOException e) {

                        e.printStackTrace();
                        return null;
                    }
                }
            } while ((index = indexMemoryByte.getAttachedSlot(index)) != 0);
        } finally {
            unlock();
        }
        return null;
    }

    /**
     * 根据 key字节查找判断是否存在
     *
     * @return
     */
    public boolean contains(byte[] key) {

        return get(key) == null ? false : true;
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
     * 根据offset去获取key的bytes,由于offset指itemData的开始段
     * <pre><b>itemData格式:</b> itemData.length(4字节) + key.length(4字节) + key/value(n字节)</pre>
     *
     * @param offset： itemData的开头
     * @return
     * @throws IOException
     */
    private byte[] keyData(long offset) throws IOException {

        // 获取key数据的长度
        int keyLength = NumberPacker.unpackInt(fsData.seek(offset + 4, 4));

        return fsData.readSequentially(keyLength);
    }

    /**
     * 索引文件达到full即attachedSlots没有可以再利用 需要扩容
     * 扩容的时候：从磁盘文件里顺序读取文件判断是否该数据被删除
     * 并写到另一个文件里
     *
     * @throws Exception
     */
    private void resize() throws Exception {

        int newCap = 0;

        if ((newCap = indexMemoryByte.capacity() << 1) > MAXSIZE) {
            throw new IllegalArgumentException(
                    "The comapacity:`" + newCap + "` is reaching its maxsize and can`t expend anymore");
        }

        // 用来读文件的缓存的block
        ReadingBufferedBlock readingBlock = ReadingBufferedBlock.allocate(BufferedSize);
        // 用来写文件的缓存的block
        WritingBufferedBlock writingBlock = WritingBufferedBlock.allocate(BufferedSize);
        // 将写文件的缓存block的limit设置为最大
        writingBlock.setLimit(BufferedSize);

        // 文件用来写去除无用数据
        InputOutData temData = fsd.createDataStream(fileName + TEMFILESUFFIX, false);
        IndexMemoryByte tempMemoryByte = IndexMemoryByte.init(newCap, 0);

        // 每次resize之前，先将文件的指针置于开头的位置，便于从头开始顺序读
        fsData.jumpHeader();
        while (readingBlock.placeHeader()
                .setLimit(fsData.readBlock(readingBlock.getBlock())) != -1) {
            byte[] resultData;
            while ((resultData = readingBlock.nextItem()) != null) {
                byte[] keyBytes = Bytes.extractKey(resultData);
                long Offset = Bytes.extractOffset(resultData);
                byte[] itemData = Bytes.extractItemData(resultData);

                // 有效的itemData数据，添加到writingBlock
                if (isItemValid(keyBytes, Offset)) {
                    long newOffset = writingBlock.getOffset();
                    putOnExtension(keyBytes, newOffset, tempMemoryByte);

                    if (writingBlock.hasRoomFor(itemData)) {
                        writingBlock.wrap(itemData);
                    } else {
                        temData.append(writingBlock.flush());
                        writingBlock.wrap(itemData);
                    }
                }
            }
        }
        temData.append(writingBlock.flush());

        // 删除旧的文件
        fsData.deleteFile();
        temData.reName(fileName + DATASUFFIX);
        fsData = temData;
        indexMemoryByte = tempMemoryByte;
    }

    /**
     * 不加锁的put 只在扩容或者campact的时候调用,添加新的索引信息
     *
     * @param key         存储的key 字节数组
     * @param offset      rawdata在磁盘文件的偏移
     * @param indexMemory 另一内存索引用来扩充等等
     */
    private void putOnExtension(byte[] key, long offset, IndexMemoryByte indexMemory) {

        int index = Hash.FNVHash1(key) & (indexMemory.capacity() - 1);
        int hc = Hash.KeyHash(key);
        // 标记最终的slot是否在attachedSlots里
        boolean isInAttached = false;

        if (indexMemory.getHashCode(index) != 0) {
            isInAttached = true;

            while (indexMemory.getAttachedSlot(index) != 0) {
                index = indexMemory.getAttachedSlot(index);
            }
        }

        // 新建slot, 并将slot的index 更新到上一个slot的attachedSlot
        if (isInAttached) {
            int newIndex = indexMemory.nextCurrentSafely();
            indexMemory.setAttachedSlot(index, newIndex);
            indexMemory.replaceSlot(hc, offset, 0, newIndex);
            return;
        }
        indexMemory.replaceSlot(hc, offset, 0, index);
    }

    /**
     * 从磁盘读取每个item，并在索引内存里判断其是否有效
     *
     * @return true: 无效的item； false：有效的item
     */
    private boolean isItemValid(byte[] key, long offset) {

        int index = Hash.FNVHash1(key) & indexMemoryByte.capacity() - 1;

        do {
            if (indexMemoryByte.getFilePosition(index) == offset) {
                return true;
            }
        } while ((index = indexMemoryByte.getAttachedSlot(index)) != 0);

        return false;
    }

    /**
     * 将内存的index文件flush到磁盘里
     */
    private void close() {
        // 写入索引的容量值 attachedSlots用到的current值
        indexMemoryByte.writeCapacity()
                .writeCurrent();

        try {
            fsIndex.deleteFile()
                    .createNewFile()
                    .flush(indexMemoryByte.Bytes());
        } catch (IOException e) {

            e.printStackTrace();
        }
        indexMemoryByte.release();
    }
}
