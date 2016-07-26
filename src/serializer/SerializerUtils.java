package serializer;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.tools.corba.se.idl.constExpr.BooleanNot;

import java.util.HashMap;

/**
 * Created by funeyu on 16/7/26.
 */
class SerializerUtils {

    private final static HashMap<Class, Boolean>  primitivesMap = new HashMap();

    static {
        primitivesMap.put(Integer.class, Boolean.TRUE);
        primitivesMap.put(Float.class, Boolean.TRUE);
        primitivesMap.put(Long.class, Boolean.TRUE);
        primitivesMap.put(Short.class, Boolean.TRUE);
        primitivesMap.put(Double.class, Boolean.TRUE);
        primitivesMap.put(Boolean.class, Boolean.TRUE);
        primitivesMap.put(Byte.class, Boolean.TRUE);
    }

    public static boolean isPrimitive(Class clazz) {
        if(clazz.isPrimitive()) {
            return true;
        }
        if(primitivesMap.containsKey(clazz)) {
            return true;
        }
        return false;
    }
}
