import com.hive.udf.geohash.UDFGeohashDecode;
import com.hive.udf.geohash.UDFGeohashEncode;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestGeohash {
    @Test
    public  void testUDFGeohashEncode() {
        UDFGeohashEncode udf = new com.hive.udf.geohash.UDFGeohashEncode();
        DoubleWritable lng = new  DoubleWritable(118.36802);

        DoubleWritable lat = new  DoubleWritable(31.32922);
        IntWritable pres = new IntWritable(7);
        String results1 = String.valueOf((udf.evaluate(lng,lat,pres)));
        assertEquals("wts4jx2",results1);
    }



//    @Test
//    public  void testUDFGeohashDecode() {
//        UDFGeohashDecode udf = new com.hive.udf.geohash.UDFGeohashDecode();
//        Text vgeohash= new Text("wts4jx2");
//        Text res =new Text("118.36802" + "," + "31.32922");
//        String results1 = String.valueOf((udf.evaluate(vgeohash)));
//        assertEquals(res,results1);
//    }

}
