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
         * 用于String => bytes
         * @param String
         * @return
         */
        private byte[] toBytes(String str) {

            return str.getBytes();
        }

        /**
         * byte => String
         * @param bytes
         * @return
         */
        private String toChars(byte[] bytes) {

            return new String(bytes);
        }
    }
}
