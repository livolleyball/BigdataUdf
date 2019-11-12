package com.utils;

import org.apache.commons.lang.StringUtils;

public class CharByt
{

    /**
     * 半角转全角
     *
     * @param str
     * @return
     *

     * @Author lihuamin
     * @Date 2019-11-11 11:17
     * @Version 1.0
     */
    public static String ToFullWidth(String str) {
        if(StringUtils.isEmpty(str)){
            return null;
        }
        char c[] = str.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == ' ') {
                c[i] = '\u3000';
            } else if (c[i] < '\177') {
                c[i] = (char) (c[i] + 65248);

            }
        }
        return new String(c);
    }

    /**
     * 全角转半角
     *
     * @param str
     * @return
     *

     * @Author lihuamin
     * @Date 2019-11-11 11:17
     * @Version 1.0

     */
    public static String ToHalfWidth(String str) {
        char c[] = str.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == '\u3000') {
                c[i] = ' ';
            } else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
                c[i] = (char) (c[i] - 65248);

            }
        }
        String returnString = new String(c);
        return returnString;
    }

    public static void main(String[] args) {
        String str = "８１４乡道阿斯蒂芬１２３／．１２，４１２看２家１快２看了就２；看了２叫看来＋看来家１２考虑就２３；了３接口２了２会２，．水电费苦辣时间的２　　１２５１２３１２３１２１２０９－０２１～！＠＃＄％＾＆＊（）＿";
        String result = ToHalfWidth(str);
        System.out.println(result);
        String newstr=ToFullWidth(result);
        System.out.println(newstr);
    }


}
