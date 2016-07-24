package serializer;

import java.nio.ByteBuffer;

/**
 * Created by funeyu on 16/7/24.
 */
public class SerializerImp implements Serializer {

    @Override
    public <T> byte[] serialize(T obj) {

        return new byte[0];
    }

    @Override
    public <T> byte[] derialize(byte[] data, Class<T> clazz) {
        return new byte[0];
    }

    private static class StringSerializer implements Serializer {

        @Override
        public <T> byte[] serialize(T obj) {
            if(!(obj instanceof String)) {
                throw new IllegalArgumentException("Only Strings are supported");
            }
            return new byte[0];
        }

        @Override
        public <T> byte[] derialize(byte[] data, Class<T> clazz) {
            return new byte[0];
        }


        /**
         * 将char[]类型的数据转换成byte, 用于String => bytes
         * @param chars
         * @return
         */
        private byte[] toBytes(char[] chars) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(chars.length * 2);
            for (char c : chars) {
                byteBuffer.putChar(c);
            }

            return byteBuffer.array();
        }

        /**
         * 将byte数组转换成char类型数组用来byte => String
         * @param bytes
         * @return
         */
        private char[] toChars(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            char[] chars = new char[bytes.length / 2];
            for (int i = 0, length = chars.length; i < length; i ++) {
                chars[i] = byteBuffer.getChar();
            }

            return chars;
        }
    }
}
