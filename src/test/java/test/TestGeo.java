package test;

import com.hive.udf.geo.GenericUDFGCJ02ToBd09;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.HashMap;

public class TestGeo {

    @Test
    public void testGenericUDFGCJ02ToBd09() throws HiveException {
        GenericUDFGCJ02ToBd09 udf = new GenericUDFGCJ02ToBd09();
        ObjectInspector lng = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ObjectInspector lat = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;;
        ObjectInspector resultInspector = udf.initialize(new ObjectInspector[]{lng, lat});

        HashMap<String, Double> map_res = udf.evaluate(new GenericUDF.DeferredObject[]
                {new GenericUDF.DeferredJavaObject(117.008122),
                        new GenericUDF.DeferredJavaObject(40.584112)});

        System.out.println(map_res);
        System.out.println("通过Map.keySet遍历key和value：");
        for (String key : map_res.keySet()) {
            System.out.println("key= "+ key + " and value= " + map_res.get(key));
        }
    }
}
