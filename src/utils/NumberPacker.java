package utils;

import java.io.DataInput;
import java.io.IOException;

/**
 * number与byte array的互相转换的工具类 note:这里的转换int 与long型number都是non-negative的
 *
 * @author funeyu
 */
public final class NumberPacker {

    private NumberPacker() {
    }

    /**
     * 将long型数字打包成字节数组
     *
     * @param ba    long pack成的字节数组
     * @param value 需要pack的long
     * @return
     */
    public static byte[] packLong(long value) {

        byte[] ba = new byte[5];
        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        int i = 1;
        while ((value & ~0x7FL) != 0) {
            ba[i - 1] = (byte) (((int) value & 0x7F) | 0x80);
            value >>>= 7;
            i++;
        }
        ba[i - 1] = (byte) value;
        return ba;
    }

    /**
     * 将一定的字节数组解包成long数字
     *
     * @param bytes
     * @return
     */
    public static long unpackLong(byte[] bytes) {

        long result = 0;
        int index = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = bytes[index++];
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }

    /**
     * 将字符数组转换成int
     *
     * @param bytes
     * @return int
     */
    public static int unpackInt(byte[] bytes) {

        int result = 0;
        for (int i = 0; i < 4; i++) {
            result |= ((bytes[i] & 0xFF) << (4 - 1 - i) * 8);
        }

        return result;
    }

    /**
     * 将int number转换成字节数组
     *
     * @param number
     * @return 长度为4的字节数组
     */
    public static byte[] packInt(int value) {

        byte[] ba = new byte[4];

        for (int i = 0; i < 4; i++) {
            ba[i] = (byte) ((value >> 8 * (4 - 1 - i)) & 0xFF);
        }
        return ba;
    }

    public static void main(String[] args) {
        byte[] bytes = packInt(698446115);
        System.out.println(unpackInt(bytes));

    }
}
