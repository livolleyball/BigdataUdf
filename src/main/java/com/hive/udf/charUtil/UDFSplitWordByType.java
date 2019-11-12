package com.hive.udf.charUtil;

import com.utils.SplitWordHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

/**
 * @Author lihuamin
 * @Date 2019-11-12 18:25
 * @Version 1.0
 **/
public class UDFSplitWordByType extends UDF {
    private static final Log LOG = LogFactory.getLog(UDFSplitWordByType.class.getName());
    private boolean warned = false;

    public HashMap<String, String> evaluate(String str) {

        if (str == null) {
            return null;
        }
        return (HashMap<String, String>) SplitWordHelper.SplitWordByType(str);
    }
}

