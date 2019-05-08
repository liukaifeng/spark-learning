package com.lkf.cdh;

import org.apache.livy.client.ext.model.DateUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-04-15 15-17
 */
public class DateDemo {
    public static void main( String[] args ) {

        System.out.println(convertTimeToLong("2018-11-25"));
    }

    public static Long convertTimeToLong(String time) {
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime parse = LocalDateTime.parse(time, ftf);
        return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toEpochSecond();
    }

}
