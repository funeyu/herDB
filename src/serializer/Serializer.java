package serializer;

/**
 * Created by funeyu on 16/7/23.
 */
interface Serializer {

    <T> byte[] serialize(T obj);

    <T> T deserialize(byte[] data);
}
