package org.herDB.serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by funeyu on 16/7/23.
 */
enum PrimitiveType {
    //
//    这样写是不对的
//    INT {
//        @Override int weight() {
//            return 4;
//        }
//        @Override Class classify() {
//            return Integer.class;
//        }
//        @Override <Integer> byte[] serialize(Integer value) {
//            ByteBuffer bytes = ByteBuffer.allocate(1 + weight());
//            bytes.put((byte)id());
//            bytes.putInt(((Integer)value).intValue());
//            return bytes.array();
//        }
//        @Override Integer deserialize(byte[] bytes, int off, int len) {
//            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
//            int result = byteBuffer.getInt();
//            return new Integer(result);
//        }
//        @Override int id() {
//            return 1;
//        }
//
//    },
    INT {
        @Override
        int weight() {
            return 4;
        }

        @Override
        Class classify() {
            return Integer.class;
        }

        @Override
        <T> byte[] serialize(T value) {

            ByteBuffer bytes = ByteBuffer.allocate(1 + weight());
            bytes.put((byte) id());
            bytes.putInt(((Integer) value).intValue());
            return bytes.array();
        }

        @Override
        Integer deserialize(byte[] bytes, int off, int len) {

            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
            int result = byteBuffer.getInt();
            return new Integer(result);
        }

        @Override
        int id() {
            return 1;
        }

    },
    LONG {
        @Override
        int weight() {
            return 8;
        }

        @Override
        Class classify() {
            return Long.class;
        }

        @Override
        <T> byte[] serialize(T obj) {
            ByteBuffer bytes = ByteBuffer.allocate(1 + weight());
            bytes.put((byte) id());
            bytes.putLong(((Long) obj).longValue());
            return bytes.array();
        }

        @Override
        int id() {
            return 2;
        }

        @Override
        Long deserialize(byte[] bytes, int off, int len) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
            long result = byteBuffer.getLong();
            return new Long(result);
        }
    },
    SHORT {
        @Override
        int weight() {
            return 2;
        }

        @Override
        Class classify() {
            return Short.class;
        }

        @Override
        <T> byte[] serialize(T obj) {
            ByteBuffer bytes = ByteBuffer.allocate(1 + weight());
            bytes.put((byte) id());
            bytes.putShort(((Short) obj).shortValue());
            return bytes.array();
        }

        @Override
        int id() {
            return 3;
        }

        @Override
        Short deserialize(byte[] bytes, int off, int len) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
            short result = byteBuffer.getShort();
            return new Short(result);
        }
    },
    BOOLEAN {
        @Override
        int weight() {
            return 1;
        }

        @Override
        Class classify() {
            return Boolean.class;
        }

        @Override
        <T> byte[] serialize(T obj) {
            ByteBuffer bytes = ByteBuffer.allocate(1 + weight());
            bytes.put((byte) id());
            bytes.put(((Boolean) obj).booleanValue() ? (byte) 1 : (byte) 0);
            return bytes.array();
        }

        @Override
        int id() {
            return 4;
        }

        @Override
        Boolean deserialize(byte[] bytes, int off, int len) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
            boolean result = byteBuffer.get() == (byte) 1 ? true : false;
            return new Boolean(result);
        }
    },
    FLOAT {
        @Override
        int weight() {
            return 4;
        }

        @Override
        Class classify() {
            return Float.class;
        }

        @Override
        <T> byte[] serialize(T obj) {
            ByteBuffer bytes = ByteBuffer.allocate(1 + weight());
            bytes.put((byte) id());
            bytes.putFloat(((Float) obj).floatValue());
            return bytes.array();
        }

        @Override
        int id() {
            return 5;
        }

        @Override
        Float deserialize(byte[] bytes, int off, int len) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
            float result = byteBuffer.getFloat();
            return new Float(result);
        }
    },
    INTARRAY {
        @Override
        int weight() {
            return 4;
        }

        @Override
        Class classify() {
            return (new int[]{1}).getClass();
        }

        @Override
        <T> byte[] serialize(T obj) {
            int[] intArray = (int[]) obj;
            ByteBuffer bytes = ByteBuffer.allocate(1 + intArray.length * weight());
            bytes.put((byte) id());

            for (int i = 0, length = intArray.length; i < length; i++) {
                bytes.putInt(intArray[i]);
            }
            return bytes.array();
        }

        @Override
        int id() {
            return 101;
        }

        @Override
        int[] deserialize(byte[] bytes, int off, int len) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, off, len);
            int[] results = new int[len >> 2];
            for (int i = 0, length = results.length; i < length; i++) {
                results[i] = byteBuffer.getInt();
            }
            return results;
        }
    };

    // 根据Class类型去获取相应的PrimitiveType
    public final static HashMap<Class, PrimitiveType> EnumsClassMap = new HashMap();

    public final static HashMap<Integer, PrimitiveType> EnumIdMap = new HashMap();

    static {
        for (PrimitiveType type : PrimitiveType.values()) {
            EnumsClassMap.put(type.classify(), type);
            EnumIdMap.put(new Integer(type.id()), type);
        }
    }

    /**
     * 该类型数据占据的byte数
     *
     * @return
     */
    abstract int weight();

    /**
     * 返回Enum所在的Class类型
     *
     * @return
     */
    abstract Class classify();

    /**
     * 将对象序列化成byte类型的数据
     *
     * @param obj 待序列化的对象
     * @param <T> 泛型 T
     * @return obj => bytes
     */
    abstract <T> byte[] serialize(T obj);

    /**
     * 每个枚举数据都返回独自的id;
     * 用来存储在磁盘文件中,标记Class类型
     *
     * @return
     */
    abstract int id();

    /**
     * 从bytes中解码成相应的T对象数据
     *
     * @param bytes
     * @param off
     * @param len
     * @param <T>
     * @return
     */
    abstract <T> T deserialize(byte[] bytes, int off, int len);
}
