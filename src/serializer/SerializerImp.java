package serializer;

/**
 * Created by funeyu on 16/7/24.
 */
public final class SerializerImp implements Serializer {

    private final StringSerializer stringSerializer = new StringSerializer();

    private SerializerImp() {

    }

    public static SerializerImp build() {
        return new SerializerImp();
    }

    @Override public <T> byte[] serialize(T obj) {
        if (obj == null) {
            return new byte[0];
        }
        if(obj instanceof String) {
            return stringSerializer.serialize(obj);
        }
        // 判断是否为基本类型的数据,note: Integer.class.isPrimitive()返回为false;
        if(! SerializerUtils.isPrimitive(obj.getClass())) {
            System.out.println(obj.getClass());
            throw new IllegalArgumentException("only String and primitive value are supported!");
        }

        return PrimitiveType.EnumsClassMap.get(obj.getClass())
                            .serialize(obj);
    }

    @Override public <T> T deserialize(byte[] data) {
        byte id = data[0];
        if(id == (byte)stringSerializer.id) {
            // 为String类型的数据
            return (T)stringSerializer.deserialize(data);
        }
        T result = PrimitiveType.EnumIdMap.get(new Integer(id))
                   .deserialize(data, 1, data.length - 1);
        return result;
    }


    private static class StringSerializer implements Serializer {

        // 存储在磁盘中,为string的标记
        private int id = 255;
        private StringSerializer() {}

        @Override public<String> byte[] serialize(String obj) {
            byte[] objBytes = toBytes((java.lang.String) obj);
            byte[] resultBytes = new byte[objBytes.length + 1];
            System.arraycopy(new byte[]{(byte)id}, 0, resultBytes, 0, 1);
            System.arraycopy(objBytes, 0, resultBytes, 1, objBytes.length);
            return resultBytes;
        }

        @Override public String deserialize(byte[] data) {
            byte[] dataBytes = new byte[data.length - 1];
            System.arraycopy(data, 1, dataBytes, 0, dataBytes.length);
            return new String(dataBytes);
        }

        private byte[] toBytes(String str) {

            return str.getBytes();
        }
    }
}
