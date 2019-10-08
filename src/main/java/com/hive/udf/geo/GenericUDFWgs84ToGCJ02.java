package com.hive.udf.geo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

import java.util.HashMap;

import static com.utils.GeoUtils.WGS84ToGCJ02;

public class GenericUDFWgs84ToGCJ02 extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function UDFWgs84ToGCJ02(lng,lat) takes exactly 2 arguments.");
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


        HashMap<String, Double> wgs_gcj = null;
        try {
            wgs_gcj = (HashMap<String, Double>) WGS84ToGCJ02(gcjLat.get(), gcjLng.get());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return wgs_gcj;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
