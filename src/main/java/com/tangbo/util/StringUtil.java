package com.tangbo.util;

/**
 * Created by Li.Xiaochuan on 16/11/1.
 */
public class StringUtil {

    public static String trimToEmpty(String str) {
        if(str == null) {
            return "";
        } else {
            return str.trim();
        }
    }
}
