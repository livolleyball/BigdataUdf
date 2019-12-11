import com.hive.udf.base64.UDFDecodeBase64;
import com.hive.udf.base64.UDFEncodeBase64;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static junit.framework.Assert.assertEquals;

/**
 * @author lihuamin
 * @version 1.0
 * @date 2019/12/11 4:34 下午
 **/


public class TestBase64 {
    public static void main(String[] args) {
        String text = "base64 in java8 lib";
        //编码
        String encode = Base64.getEncoder()
                .encodeToString(text.getBytes(StandardCharsets.UTF_8));
        System.out.println(encode);

        //解码
        String decode = new String(Base64.getDecoder().decode(encode), StandardCharsets.UTF_8);
        System.out.println(decode);
    }

    @Test
    public void testUDFDecodeBase64() throws Exception {
        UDFDecodeBase64 udf = new UDFDecodeBase64();

        String results = String.valueOf((udf.evaluate("YmFzZTY0IGluIGphdmE4LWhpdmUgbGli")));

        System.out.println(results);
        assertEquals("base64 in java8-hive lib",results);
    }


    @Test
    public void testUDFEncodeBase64() throws Exception {
        UDFEncodeBase64 udf = new UDFEncodeBase64();

        String results = String.valueOf((udf.evaluate("base64 in java8-hive lib")));
        String results1 = String.valueOf((udf.evaluate("hive")));
        System.out.println(results);
        System.out.println(results1);
        assertEquals("YmFzZTY0IGluIGphdmE4LWhpdmUgbGli",results);
    }
}
