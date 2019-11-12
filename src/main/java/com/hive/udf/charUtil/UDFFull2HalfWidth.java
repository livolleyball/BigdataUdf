package com.hive.udf.charUtil;

import com.utils.CharByt;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * @Author lihuamin
 * @Date 2019-11-12 18:08
 * @Version 1.0
 **/

@Description(name = "UDFFull2HalfWidth",
        value = "_FUNC_(Full Width String) - return Half Width String")
public class UDFFull2HalfWidth extends UDF {
    public Text evaluate(Text fullWidth){

        if (fullWidth == null ) {
            return null;
        }
        return new Text(CharByt.ToHalfWidth(String.valueOf(fullWidth)));
    }
}
