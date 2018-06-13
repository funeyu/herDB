package org.herDB.index;

import org.herDB.utils.NumberPacker;

import java.util.Arrays;


/**
 * 内存字节数组的类；其字节数组长度为： capacity * 2 * SlotSize + 8
 *
 * @author funeyu
 */
public final class IndexMemoryByte {

    // 内存索引的全部字节数据 
    private byte[] bytes;
    // 这个值用来与做&hash计算得出index, 为2^n
    private int capacity;
    // 标记attachedSlots里用到哪一个attachedSlot
    private int current;

    private IndexMemoryByte(int capacity, int current) {

        this.capacity = capacity;
        this.current = current;
        this.bytes = new byte[(capacity << 1) * Slot.slotSize + 8];
    }

    private IndexMemoryByte(int capacity, int current, byte[] bytes) {

        this.capacity = capacity;
        this.current = current;
        this.bytes = bytes;
    }

    /**
     * 初始一个空的IndexMemory的容器;
     *
     * @param capacity
     * @param current
     * @return
     */
    public static IndexMemoryByte init(int capacity, int current) {

        return new IndexMemoryByte(capacity, current);
    }

    /**
     * 通过bytes 数组生成一个IndexMemoryByte
     *
     * @param bytes
     * @return
     */
    public static IndexMemoryByte open(byte[] bytes) {

        int capacity = NumberPacker.unpackInt(new byte[]{
                bytes[0],
                bytes[1],
                bytes[2],
                bytes[3]
        });

        int current = NumberPacker.unpackInt(new byte[]{
                bytes[5],
                bytes[6],
                bytes[7],
                bytes[8]
        });

        return new IndexMemoryByte(capacity, current, bytes);
    }

    /**
     * 根据index获取一个slot的内存字节数组
     *
     * @param index
     * @return
     */
    public byte[] slotBytes(int index) {

        return Arrays.copyOfRange(bytes, index * Slot.slotSize + 8,
                (index + 1) * Slot.slotSize + 8);
    }

    /**
     * 根据hash函数计算出来的hash值，去计算slot的序号
     *
     * @param hash
     * @return 对应的slot的序号
     */
    public int slotIndexFor(int hash) {

        return hash & (capacity - 1);
    }

    /**
     * 根据index获取hashcode
     *
     * @param index: 为slot的索引值
     * @return
     */
    public int getHashCode(int index) {

        return Slot.getHashCode(slotBytes(index));
    }

    /**
     * 根据index获取索引中,value在文件中偏移
     *
     * @param index
     * @return
     */
    public long getFilePosition(int index) {

        return Slot.getFileInfo(slotBytes(index));
    }

    /**
     * 根据index获取与之相邻的后继Slot的索引值
     *
     * @param index
     * @return
     */
    public int getAttachedSlot(int index) {

        return Slot.getAttachedSlot(slotBytes(index));
    }

    /**
     * 设置preIndex的attachedSlot序号为：thisIndex
     *
     * @param preIndex
     * @param thisIndex
     */
    public void setAttachedSlot(int preIndex, int thisIndex) {

        byte[] attachedSlotIndex = NumberPacker.packInt(thisIndex);
        for (int i = 0; i < 4; i++) {
            bytes[preIndex * Slot.slotSize + 8 + 9 + i] = attachedSlotIndex[i];
        }
    }

    /**
     * 将序号为index的slot的数据信息改成新的哈希，文件指针， 后继slot
     *
     * @param hc    hashCode
     * @param fp    filePosition
     * @param as    attachedSlot
     * @param index
     */
    public void replaceSlot(int hc, long fp, int as, int index) {

        byte[] slotBytes = Slot.generate(hc, fp, as);
        for (int i = 0, length = slotBytes.length; i < length; i++) {
            bytes[index * Slot.slotSize + 8 + i] = slotBytes[i];
        }
    }

    // 返回内存索引的capacity
    public int capacity() {

        return capacity;
    }

    // 将current自增
    public void incCurrent() throws IndexOutofRangeException {

        if (++current > capacity) {
            throw new IndexOutofRangeException();
        }

    }

    /**
     * 在attachedSlots里获取current 下一个slot，
     * 每获取一次current 都要自增一次
     * <pre>
     * attachedSlots full的时候，throws Exception
     * </pre>
     *
     * @return
     * @throws Exception
     */
    public int nextCurrent() throws IndexOutofRangeException {

        int curr = current;

        incCurrent();
        return curr + capacity;
    }

    /**
     * 在attachedSlots获取current值;
     * 因为一般在扩容的新的索引内存数据，不会有IndexOutOfRangeException
     *
     * @return
     */
    public int nextCurrentSafely() {

        int oldIndex = current;
        current++;
        return oldIndex + capacity;
    }

    // 返回该内存索引的所有数据
    public byte[] Bytes() {

        return bytes;
    }

    // 写入capacity到bytes
    public IndexMemoryByte writeCapacity() {

        byte[] capacityBytes = NumberPacker.packInt(capacity);
        for (int i = 0; i < 4; i++) {
            bytes[i] = capacityBytes[i];
        }

        return this;
    }

    // 写入attachedSlots用到哪一个slot
    public IndexMemoryByte writeCurrent() {

        byte[] currentBytes = NumberPacker.packInt(current);
        for (int i = 0; i < 4; i++) {
            bytes[i + 4] = currentBytes[i];
        }

        return this;
    }

    // 释放相应的内存
    // to do: check it's fine or not to call system.gc() 
    public void release() {

        bytes = null;
        System.gc();
    }
}
