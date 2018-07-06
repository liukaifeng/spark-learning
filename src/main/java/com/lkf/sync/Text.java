package com.lkf.sync;

import java.util.Set;
import java.util.StringTokenizer;

/**
 * @package: com.lkf.sync
 * @project-name: spark-learning
 * @description: todo 一句话描述该类的用途
 * @author: Created by 刘凯峰
 * @create-datetime: 2018-07-02 19-59
 */
public class Text {
    public enum Style {
        BOLD, ITALIC, UNDERLINE, STRIKETHROUGH
    }

    public void applyStyles( Set<Style> styles ) {
        System.out.println(styles);
    }

    public static void main( String[] args ) {
        StringTokenizer st = new StringTokenizer("this#is a #test","#");
        while (st.hasMoreTokens()) {
            System.out.println(st.nextToken());
        }
//        new Text().applyStyles(EnumSet.of(Style.BOLD, Style.ITALIC));

//        try {
//            System.out.println(InetAddress.getLocalHost().getCanonicalHostName());
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
    }


}
