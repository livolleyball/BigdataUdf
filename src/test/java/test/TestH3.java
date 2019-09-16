package test;

import com.hive.udf.h3.GenericUDFH3kRing;
import com.hive.udf.h3.UDFH3Encode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//import com.hive.udf.h3.UDFH3kRing;


public class TestH3 {
    @Test
    public  void testUDFH3Encode(){
        UDFH3Encode udf = new UDFH3Encode();
        DoubleWritable lng = new  DoubleWritable(118.36802);
        DoubleWritable lat = new  DoubleWritable(31.32922);
        IntWritable pres = new IntWritable(7);
        IntWritable pres1 = new IntWritable(4);

        DoubleWritable lng1 = new  DoubleWritable(103.9511186);
        DoubleWritable lat1 = new  DoubleWritable(30.59439389);
        DoubleWritable lng2 = new  DoubleWritable(103.9509);
        DoubleWritable lat2 = new  DoubleWritable(30.60176);
        
  
        String results = String.valueOf((udf.evaluate(lng,lat,pres)));
        String results1 = String.valueOf((udf.evaluate(lng1,lat1,pres1)));
        String results2 = String.valueOf((udf.evaluate(lng2,lat2,pres1)));
        System.out.println(results1+"    "+results2);
        assertEquals("873093c4effffff",results);
        

    }

    @Test
    public void testUDFH3kRing() throws HiveException {
        GenericUDFH3kRing udf = new com.hive.udf.h3.GenericUDFH3kRing();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;;
        ObjectInspector resultInspector = udf.initialize(new ObjectInspector[]{stringOI, intOI});

        Object hexagons = udf.evaluate(new GenericUDF.DeferredObject[]
                {new GenericUDF.DeferredJavaObject("8928308280fffff"),
                        new GenericUDF.DeferredJavaObject(1)});

        System.out.println(hexagons.toString());
//        assertEquals(1 + 6, hexagons.toString().size());
        assertTrue(hexagons.toString().contains("8928308280fffff"));
        assertTrue(hexagons.toString().contains("8928308280bffff"));
        assertTrue(hexagons.toString().contains("89283082807ffff"));
        assertTrue(hexagons.toString().contains("89283082877ffff"));
        assertTrue(hexagons.toString().contains("89283082803ffff"));
        assertTrue(hexagons.toString().contains("89283082873ffff"));
        assertTrue(hexagons.toString().contains("8928308283bffff"));

    }

}
