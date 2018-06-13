package org.herDB.index;

import java.util.Arrays;

import org.herDB.utils.NumberPacker;

/**
 * 为哈希的槽节点相关的方法集合，每个slot实际上三步分组成
 * <pre>
 * <--hashcode-->|<--fileposition-->|<--attachedslot-->
 * ----4字节-----------5字节----------------4字节--------
 * </pre>
 * <p>
 * 此时的Slot相当于static unit class
 *
 * @author funeyu
 */
public final class Slot {
    // 每个slot
    public final static int slotSize = (4 + 5 + 4);

    private Slot() {
    }

    ;

    /**
     * 由hc fp as三个参数去组装成一个byte[slotSize]
     *
     * @param hashcode
     * @param fileposition 5 byte长度的long数 最大值为 256^5 1T
     * @param attachedslot
     */
    public static byte[] generate(int hashcode, long fileposition, int attachedslot) {

        byte[] bytes = new byte[slotSize];

        byte[] hc = NumberPacker.packInt(hashcode);
        for (int i = 0; i < 4; i++)
            bytes[i] = hc[i];

        byte[] fp = NumberPacker.packLong(fileposition);
        for (int i = 0; i < 5; i++)
            bytes[i + 4] = fp[i];

        byte[] as = NumberPacker.packInt(attachedslot);
        for (int i = 0; i < 4; i++)
            bytes[i + 9] = as[i];

        return bytes;
    }

    /**
     * 设置索引号为slot的槽attachedSlot 值为attachedslot
     *
     * @param slot
     * @param attachedslot
     * @param bytes        为索引的全量内存数据
     */
    public static void setAttachedSlot(int slot, int attachedslot, byte[] bytes) {

        byte[] attach = NumberPacker.packInt(attachedslot);
        for (int i = 0; i < 4; i++) {
            bytes[slot * slotSize + 8 + 9 + i] = attach[i];
        }
    }


    /**
     * 获取该slot下 相对应key的hashcode
     *
     * @param bytes
     * @return
     */
    public static int getHashCode(byte[] bytes) {

        return NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 0, 4));
    }

    /**
     * 获取hash索引某slots对应的file文件位置信息
     *
     * @param bytes
     * @return
     */
    public static long getFileInfo(byte[] bytes) {

        return NumberPacker.unpackLong(Arrays.copyOfRange(bytes, 4, 9));
    }

    /**
     * 获取该slot的后继slot的id
     *
     * @param bytes
     * @return
     */
    public static int getAttachedSlot(byte[] bytes) {

        return NumberPacker.unpackInt(Arrays.copyOfRange(bytes, 9, slotSize));
    }
}
