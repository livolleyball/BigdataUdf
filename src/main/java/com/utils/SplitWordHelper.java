package com.utils;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @Author lihuamin
 * @Date 2019-11-11 11:17
 * @Version 1.0
 **/
public class SplitWordHelper {
    public static Map<String, String> SplitWordByType(String str) {
        char c[] = str.toCharArray();

        StringBuilder cn=new StringBuilder("");

        StringBuilder num=new StringBuilder("");
        StringBuilder en =new StringBuilder("");
//        "[^\u4e00-\u9fa5]";
        for (int i = 0; i < c.length; i++) {
            if (c[i] == '\u3000' | c[i] == ' ') {
                cn.append(" ");
                num.append(" ");
                en.append(" ");

            } else if (c[i] >= '\u4e00' && c[i] <= '\u9fa5') {
                cn.append(c[i]);
                num.append(" ");
                en.append(" ");

            }else if (c[i] >= '0' && c[i] <= '9') {
                cn.append(" ");
                num.append(c[i]);
                en.append(" ");


// a~z 97~122    A~Z 65~90
            }else if ((c[i] >= 65 && c[i] <= 90) | (c[i] >= 97 && c[i] <= 122) ) {
                cn.append(" ");
                num.append(" ");
                en.append(c[i]);

            }
        }


        Map<String, String> map = Maps.newHashMap();
        map.put("cn", cn.toString().replaceAll("\\s{2,}", " ").trim());
        map.put("en", en.toString().replaceAll("\\s{2,}", " ").trim().toLowerCase());
        map.put("num",num.toString().replaceAll("\\s{2,}", " ").trim());
//        ObjectMapper mapper = new ObjectMapper();
        return map;


    }

    public static void main(String[] args) {

        System.out.println(SplitWordByType("同一首歌KTV KTV  honey 12999 85 "));
        System.out.println(SplitWordByType("无锡彩艳公寓(7号店1楼)  "));
//        System.out.println(newstr2);

    }

}
