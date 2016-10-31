package com.tangbo.util;

/**
 * Created by Li.Xiaochuan on 16/10/30.
 */
public class TypeConverterUtil {

    @SuppressWarnings("Duplicates")
    public static int getIntValue(Object obj) {
        if(obj instanceof Byte) {
            return ((Byte) obj).intValue();
        } else if(obj instanceof Integer) {
            return ((Integer) obj).intValue();
        } else if(obj instanceof Long) {
            return ((Long) obj).intValue();
        }

        return -1;
    }

    public static long getLongValue(Object obj) {
        if(obj instanceof Byte) {
            return ((Byte) obj).longValue();
        } else if(obj instanceof Integer) {
            return ((Integer) obj).longValue();
        } else if(obj instanceof Long) {
            return ((Long) obj).longValue();
        }

        return -1;
    }
}
