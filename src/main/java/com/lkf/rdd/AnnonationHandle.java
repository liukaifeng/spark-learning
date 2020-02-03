package com.lkf.rdd;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-07-09 13-08
 */
public class AnnonationHandle<T> implements Serializable {

    Field field;
    String annonationName;
    Class<T> fieldType;

    public AnnonationHandle(Field field, String annonationName, Class<T> fieldType) {
        this.field = field;
        this.annonationName = annonationName;
        this.fieldType = fieldType;
    }
}
