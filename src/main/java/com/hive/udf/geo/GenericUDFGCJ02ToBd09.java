package com.hive.udf.geo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

import java.util.HashMap;

import static com.utils.GeoUtils.GCJ02ToBD09;

@Description(name = "UDFGCJ02ToBD09",
        value = "_FUNC_(hexAddr) - Get 谷歌/高德经纬度 to 百度经纬度.")
public class GenericUDFGCJ02ToBd09 extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;
    private MapObjectInspector mapInspector;

//
//    public HashMap<String, String> evaluate(Map<Object, Object> strMap) {
//        HashMap<String, String> newMap = new HashMap<String, String>();
//        for (Object keyObj : strMap.keySet()) {
//            newMap.put(keyObj.toString(), strMap.get(keyObj).toString());
//        }
//        return newMap;
//    }


    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function UDFGCJ02ToBD09(lng,lat) takes exactly 2 arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        converters[1] = ObjectInspectorConverters.getConverter(arguments[1],
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }

    @Override
    public HashMap<String, Double> evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);

        DoubleWritable gcjLng = (DoubleWritable) converters[0].convert(arguments[0].get());
        DoubleWritable gcjLat = (DoubleWritable) converters[1].convert(arguments[1].get());


        HashMap<String, Double> gd_bd = null;
        try {
            gd_bd = GCJ02ToBD09(gcjLat.get(), gcjLng.get());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return gd_bd;

    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}