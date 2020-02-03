package com.lkf.jvm;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-10-30 11-36
 */
public class DateDemo {
    public static void main( String[] args ) {
        //使用DateTimeFormatter获取当前周数,星期一作为一周的第一天
        WeekFields weekFields = WeekFields.of(DayOfWeek.MONDAY, 1);
        //2017年第一天
        System.out.println("2017-01-01 星期日 第 " + LocalDate.of(2017, 1, 1).get(weekFields.weekOfYear()) + " 周");
        //2017年最后一天
        System.out.println("2017-12-31 星期日 第 " + LocalDate.of(2017, 12, 31).get(weekFields.weekOfYear()) + " 周");
        //2018年第一天
        System.out.println("2018-01-01 星期一 第 " + LocalDate.of(2018, 1, 1).get(weekFields.weekOfYear()) + " 周");
        //2018年最后一天
        System.out.println("2018-12-31 星期一 第 " + LocalDate.of(2018, 12, 31).get(weekFields.weekOfYear()) + " 周");
        //2019年第一天
        System.out.println("2019-01-01 星期二 第 " + LocalDate.of(2019, 1, 1).get(weekFields.weekOfYear()) + " 周");
        //2019年最后一天
        System.out.println("2019-12-31 星期二 第 " + LocalDate.of(2019, 12, 31).get(weekFields.weekOfYear()) + " 周");
        //2020年第一天
        System.out.println("2020-01-01 星期三 第 " + LocalDate.of(2020, 1, 1).get(weekFields.weekOfYear()) + " 周");
        //2020年最后一天
        System.out.println("2020-12-31 星期四 第 " + LocalDate.of(2020, 12, 31).get(weekFields.weekOfYear()) + " 周");


        System.out.println("=================================================");
        //使用DateTimeFormatter获取当前周数
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("w");
        //2017年第一天
        System.out.println("2017-01-01 星期日 第 " + LocalDate.of(2017, 1, 1).format(dateTimeFormatter) + " 周");
        //2017年最后一天
        System.out.println("2017-12-31 星期日 第 " + LocalDate.of(2017, 12, 31).format(dateTimeFormatter) + " 周");
        //2018年第一天
        System.out.println("2018-01-01 星期一 第 " + LocalDate.of(2018, 1, 1).format(dateTimeFormatter) + " 周");
        //2018年最后一天
        System.out.println("2018-12-31 星期一 第 " + LocalDate.of(2018, 12, 31).format(dateTimeFormatter) + " 周");
        //2019年第一天
        System.out.println("2019-01-01 星期二 第 " + LocalDate.of(2019, 1, 1).format(dateTimeFormatter) + " 周");
        //2019年最后一天
        System.out.println("2019-12-31 星期二 第 " + LocalDate.of(2019, 12, 31).format(dateTimeFormatter) + " 周");
        //2020年第一天
        System.out.println("2020-01-01 星期三 第 " + LocalDate.of(2020, 1, 1).format(dateTimeFormatter) + " 周");
        //2020年最后一天
        System.out.println("2020-12-31 星期四 第 " + LocalDate.of(2020, 12, 31).format(dateTimeFormatter) + " 周");

    }
}
