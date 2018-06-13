/**
 * Created by fuheyu on 2018/6/8.
 */
import org.herDB.herdb.Configuration;
import org.herDB.herdb.HerDB;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by fuheyu on 2018/6/8.
 */
public class getTest {


    @Test
    public void getTest() throws Exception {

        HerDB herDB = HerDB.openOnlyRead("herdb");
        String v = herDB.get("java");
        Assert.assertEquals("eclipse", v);
    }
}

