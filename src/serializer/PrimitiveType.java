package serializer;

/**
 * Created by funeyu on 16/7/23.
 */
public enum PrimitiveType {

    INT {
        @Override int weight() {
            return 4;
        }
    };

    abstract int weight();
}
