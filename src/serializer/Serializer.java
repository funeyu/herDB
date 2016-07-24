package serializer;

/**
 * Created by funeyu on 16/7/23.
 */
public interface Serializer {

    <T> byte[] serialize(T obj);

    <T> byte[] derialize(byte[] data, Class<T> clazz);
}
