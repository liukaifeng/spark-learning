package com.spark;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-02-18 09-50
 */
public class DateDemo {
    private static final String yearPattern = "^2[0-9]{3}$";
    private static final String monthPattern = "^2[0-9]{3}-(0?[1-9]|1[0-2])$";
    private static final String janPattern = "(0?[13578]|1[02])-(0?[1-9]|[12][0-9]|3[01])";
    private static final String febPattern = "0?2-(0?[1-9]|[12][0-9])";
    private static final String aprPattern = "(0?[469]|11)-(0?[1-9]|[12][0-9]|30)";
    private static final String dayPattern = String.format("^2[0-9]{3}-(%s|%s|%s)$", janPattern, febPattern, aprPattern);
    private static final String hourFormat = String.format("^2[0-9]{3}-(%s|%s|%s) ([01][0-9]|2[0-3]):00:00$", febPattern, janPattern, aprPattern);
    private static final String timeFormat = String.format("^2[0-9]{3}-(%s|%s|%s) ([01][0-9]|2[0-3])(:[0-5][0-9]){2}$", febPattern, janPattern, aprPattern);

    public static void main( String[] args ) {
        System.out.println("2000".matches(yearPattern));
        System.out.println("2999".matches(yearPattern));
        System.out.println("1999".matches(yearPattern));
        System.out.println("20001".matches(yearPattern));
        System.out.println("200".matches(yearPattern));

        System.out.println("2000-11".matches(monthPattern));
        System.out.println("2000-01".matches(monthPattern));
        System.out.println("2000-1".matches(monthPattern));
        System.out.println("2000-13".matches(monthPattern));
        System.out.println("2000-00".matches(monthPattern));

        System.out.println("2000-11-1".matches(dayPattern));
        System.out.println("2000-11-01".matches(dayPattern));
        System.out.println("2000-11-31".matches(dayPattern));
        System.out.println("2000-11-00".matches(dayPattern));
        System.out.println("2000-11-29".matches(dayPattern));
        System.out.println("2000-2-29".matches(dayPattern));
        System.out.println("2000-2-30".matches(dayPattern));

        System.out.println("2000-11-1 18:60:00".matches(hourFormat));
        System.out.println("2000-11-1 18:00:10".matches(hourFormat));
        System.out.println("2000-1-1 18:00:00".matches(hourFormat));
        System.out.println("2000-11-1 28:00:00".matches(hourFormat));
        System.out.println("2000-11-1 08:00:00".matches(hourFormat));
        System.out.println("2000-11-1_18:00:00".matches(hourFormat));

        System.out.println("2000-11-1 08:00:00".matches(timeFormat));
        System.out.println("2000-11-1 08:10:00".matches(timeFormat));
        System.out.println("2000-11-1 08:00:50".matches(timeFormat));
        System.out.println("2000-11-1 08:00:70".matches(timeFormat));
    }

}
