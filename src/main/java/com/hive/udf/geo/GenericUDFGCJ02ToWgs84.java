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

import static com.utils.GeoUtils.GCJ02ExtractWGS84;

public class GenericUDFGCJ02ToWgs84  extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function UDFGCJ02ToWgs84(lng,lat) takes exactly 2 arguments.");
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


        HashMap<String, Double> gcj_wgs = null;
        try {
            gcj_wgs = GCJ02ExtractWGS84(gcjLat.get(), gcjLng.get());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return gcj_wgs;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
