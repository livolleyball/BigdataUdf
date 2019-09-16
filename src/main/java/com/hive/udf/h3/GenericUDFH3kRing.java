package com.hive.udf.h3;

import com.uber.h3core.H3Core;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Description(name = "UDFH3kRing",
        value = "_FUNC_(hexAddr) - Get hexAddr's  dist  neighboring to a Hive array of hexAddr.")
public class GenericUDFH3kRing extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;


    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function UDFH3kRing(s, int) takes exactly 2 arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[1] = ObjectInspectorConverters.getConverter(arguments[1],
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);
        H3Core h3 = null;
        Text hexAddr = (Text) converters[0].convert(arguments[0].get());
        IntWritable k = (IntWritable) converters[1].convert(arguments[1].get());
        if (hexAddr == null) {
            return null;
        }
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            ArrayList<Text> result = new ArrayList<Text>();
            List<String> hexagons = h3.kRing(String.valueOf(hexAddr), Integer.parseInt(String.valueOf(k)));
            for (String str : hexagons) {
                result.add(new Text(str));
            }
            return result;
        } catch (JSONException e) {
            return null;
        }
    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}