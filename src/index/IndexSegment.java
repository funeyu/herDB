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
    // index io操作的入口类
    private InputOutData fsIndex;
    // data io操作的入口类
    private InputOutData fsData;

    public IndexSegment(int compacity, String fileName, int current, byte[] bytes, InputOutData fs,
            InputOutData fsData) {
        this.compacity = compacity;
        this.fileName = fileName;
        this.current = current;
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
            fs.touchFile(fileName);
            return new IndexSegment((1024 << 1) * Slot.slotSize, fileName, 0, bytes, fsIndex, fsData);
        }

        byte[] bytes = fsIndex.readFully();
        byte[] fourbytes = new byte[4];
        return new IndexSegment(NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 0, 4)), fileName,
                NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 4, 8)), bytes, fsIndex, fsData);
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
            // 根据index获取该slot的byte[13]
            slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);

            while ((hc = Slot.getHashCode(slotBytes)) != 0) {
                if (hashcode == hc) {
                    oldkey = keyData(Slot.getFileInfo(slotBytes));

                    // 用新的索引数据替换旧的key数据，
                    if (Arrays.equals(oldkey, key)) {
                        attachedslot = Slot.getAttachedSlot(slotBytes);
                        byte[] newslot = Slot.generate(hashcode, fsData.maxOffSet(), attachedslot);

                        Slot.replace(bytes, index * Slot.slotSize + 8, newslot);

                        break;
                    }
                }

                if ((attachedslot = Slot.getAttachedSlot(slotBytes)) == 0) {

                    oldindex = index;
                    index = ++current + compacity;

                    // 设置上个slot 与该slot的关联
                    Slot.setAttachedSlot(oldindex, index, bytes);

                    // 新建一个slot
                    byte[] newslot = Slot.generate(hashcode, fsData.maxOffSet(), 0);

                    // 替换成新的slot
                    Slot.replace(bytes, index * Slot.slotSize + 8, newslot);
                } else {
                    index = Slot.getAttachedSlot(slotBytes);
                    slotBytes = Arrays.copyOfRange(bytes, index * Slot.slotSize + 8, (index + 1) * Slot.slotSize + 8);
                }
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

        byte[] wrapedData = new byte[key.length + value.length + 8];

        // 先写入key的长度
        byte[] keylength = NumberPacker.packInt(key.length);
        for (int i = 0; i < 4; i++) {
            wrapedData[i] = keylength[i];
        }

        // 写入key的数据
        for (int i = 0, length = key.length; i < length; i++) {
            wrapedData[i + 4] = key[i];
        }

        // 写入value的长度
        byte[] valuelength = NumberPacker.packInt(value.length);
        for (int i = 0; i < 4; i++) {
            wrapedData[key.length + 4 + i] = valuelength[i];
        }

        // 写入value的数据
        for (int i = 0, length = value.length; i < length; i++) {
            wrapedData[key.length + 8 + i] = value[i];
        }

        return wrapedData;
    }

    // 根据key查找对应的value，没有则返回null
    public byte[] get(byte[] key) {

        lock();
        try {

        } finally {
            unlock();
        }
        return null;
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
     */
    private void resize() {

    }

}
