package com.tangbo.util;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by Li.Xiaochuan on 16/10/31.
 */
public class DateTimeUtil {

    public static long getUvDayZero(long t) {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(t));
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);
        return c.getTimeInMillis() / 1000;
    }
}
