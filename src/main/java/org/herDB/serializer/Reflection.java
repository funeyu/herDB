package org.herDB.serializer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Created by funeyu on 16/7/24.
 */
public class Reflection {

    public static <T> Field[] getAllFields(T obj) {
        final Field[] fields = obj.getClass().getDeclaredFields();
        return fields;
    }

    public static Field[] getAllFields(Class clazz) {
        final Field[] fields = clazz.getDeclaredFields();
        return fields;
    }

    public static int calculateSize(Field[] fields, Object obj) {
        int size = 0;
        for (Field field : fields) {
            if(!field.getType().isPrimitive()) {
                if(field.getType().equals(String.class)) {
                    try {
                        arrageField(field, obj);
                        String str = (String) field.get(obj);
                        if (str == null)
                            size += 4;
                        else
                            size += 4 + str.length() * 2;
                    } catch (NoSuchFieldException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return 1;
    }

    public static <T> void arrageField(Field field, T obj) throws NoSuchFieldException, IllegalAccessException {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~ Modifier.FINAL);
    }
}
