package com.lkf.rdd;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-07-09 13-09
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JDBCField {
    String description() default "field name in db";

}
