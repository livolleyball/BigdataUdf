package com.hive.udf.base64;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @author lihuamin
 * @version 1.0
 * @date 2019/12/11 4:36 下午
 **/
@Description(name = "UDFFull2HalfWidth",
        value = "_FUNC_(Full Width String) - return Half Width String")

public class UDFEncodeBase64 extends UDF {
    Text result = new Text();
    public Text evaluate(String str){
        if (str == null || str.equals("")) {
            return null;
        }
        try {
            byte[] b = str.getBytes(StandardCharsets.UTF_8);
            result.set(Base64.getEncoder().encodeToString(b));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return result;
    }
}


