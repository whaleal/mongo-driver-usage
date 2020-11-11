package com.jinmu.mongodb.demo.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtil {

    public static Date getRandomDate(){
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse("1949-01-01");
            Date end = format.parse("2025-01-01");

            if(start.getTime() >= end.getTime()){
                return null;
            }
            long date = getRandom(start.getTime(),end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static long getRandom(long begin,long end){
        long rtn = begin + (long)(Math.random() * (end - begin));
        if(rtn == begin || rtn == end){
            return getRandom(begin,end);
        }
        return rtn;
    }
}
