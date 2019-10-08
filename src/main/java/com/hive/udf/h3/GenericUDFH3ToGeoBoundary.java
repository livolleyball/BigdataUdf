package com.hive.udf.h3;

import com.uber.h3core.H3Core;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GenericUDFH3ToGeoBoundary extends GenericUDF {

    private ObjectInspectorConverters.Converter[] converters;
    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private transient ArrayList<Object> ret = new ArrayList<Object>();


    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        returnOIResolver= new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The function UDFH3ToGeoBoundary(s) takes exactly 1 arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);


        ObjectInspector returnOI =
                returnOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return  ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        H3Core h3 = null;
        String hexAddr = (String) converters[0].convert(arguments[0].get());
        if (hexAddr == null) {
            return null;
        }
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<com.uber.h3core.util.GeoCoord> geoCoords = h3.h3ToGeoBoundary(hexAddr);
        try {
            ret.clear();
            for (com.uber.h3core.util.GeoCoord str : geoCoords) {
//                HashMap map_double = new HashMap();
//                map_double.put("lng", str.lng);
//                map_double.put("lat", str.lat);
                StringBuilder sb = new StringBuilder();
                sb.append(str.lng);
                sb.append(",");
                sb.append(str.lat);
                ret.add(sb);
            }
            return ret;
        } catch (JSONException e) {
            return null;
        }
    }

    public String getDisplayString(String[] strings) {
        return null;
    }


}
