package org.herDB.net;


import org.herDB.herdb.HerDB;

/**
 * Created by funeyu on 17/1/1.
 */
public class Commands {

    public static String get(String key, HerDB dbStore) {
        return dbStore.get(key);
    }

    public static void put(String key, String value, HerDB dbStore) {
        dbStore.put(key, value);
    }

}
