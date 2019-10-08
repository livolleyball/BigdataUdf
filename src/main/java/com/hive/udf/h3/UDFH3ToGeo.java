package com.hive.udf.h3;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class UDFH3ToGeo extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The function UDFH3ToGeo(s) takes exactly 1 arguments.");
        }
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }

    @Override
    public HashMap<String, Double>  evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        H3Core h3 = null;
        Text hexAddr = (Text) converters[0].convert(arguments[0].get());
        if (hexAddr == null) {
            return null;
        }
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }

        GeoCoord  coords = h3.h3ToGeo(String.valueOf(hexAddr));
        HashMap<String, Double> map_double = new HashMap();
        try {

                map_double.put("lng",coords.lng);
                map_double.put("lat",coords.lat);
            } catch (Exception ex) {
            ex.printStackTrace();
        }
        return  map_double;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
