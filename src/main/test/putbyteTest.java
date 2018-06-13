import com.sun.deploy.util.SystemUtils;
import org.herDB.herdb.Configuration;
import org.herDB.herdb.HerDB;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by fuheyu on 2018/6/8.
 */
public class putbyteTest {

    private static Configuration config;
    private static HerDB herDB;

    @BeforeClass
    public static void setUpConfig() throws Exception {
        config = Configuration.create("herdb");

        // 初始的情况下，slots的数组的大小
        config.set(Configuration.SLOTS_CAPACITY, "65536");

        // 设置读写缓冲块的大小
        config.set(Configuration.BUFFERED_BLOCK_SIZE, "8192");

        // 设置key/value数据的最大长度
        config.set(Configuration.ITEM_DATA_MAX_SIZE, "1024");

        herDB = HerDB.create(config, "herdb");
    }

    @Test
    public void putTest() {
        herDB.put("java", "eclipse");
        herDB.put("javaddddddd", "iajofdja;jfoa");
        herDB.commit();
    }

    @Test
    public void putRate() {

        long start = System.currentTimeMillis();

        for(int i = 0; i < 1024 * 1024 * 10; i ++) {
            herDB.put("java"+ i, "javajavassssss" + i);
        }

        herDB.commit();

        long end = System.currentTimeMillis();

        System.out.println("time:" + (end -start) / 1000 );
    }
}
