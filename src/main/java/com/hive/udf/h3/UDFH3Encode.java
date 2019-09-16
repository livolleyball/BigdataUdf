package com.hive.udf.h3;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import com.uber.h3core.H3Core;

import java.io.IOException;



public class UDFH3Encode extends UDF {

    public Text evaluate(DoubleWritable longitude, DoubleWritable latitude, IntWritable precision) {
        H3Core h3 = null;
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (latitude == null || longitude == null || precision == null) {
            return null;
        }
        return new Text(h3.geoToH3Address(latitude.get(), longitude.get(), precision.get()));
    }

}