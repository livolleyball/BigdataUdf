package com.hive.udf.base64;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @author lihuamin
 * @version 1.0
 * @date 2019/12/11 4:44 下午
 **/

public class UDFDecodeBase64 extends UDF {
    Text result = new Text();
    public Text evaluate(String str){
        	if (str == null || str.equals("")) {
        return null;
    }
    	try {
        byte[] b = str.getBytes();
        String decode = new String(Base64.getDecoder().decode(b), StandardCharsets.UTF_8);
        result.set(decode);
    } catch (Exception e) {
        e.printStackTrace();
        return null;
    }
    	return result;
}
}
