package com.lkf.rdd;

import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-07-09 13-07
 */
public class Jdbc2BeanUtil {

    public static <T> HashMap<String, AnnonationHandle<Class>> getSchemaMap( Class<T> classTag) {
        HashMap<String, AnnonationHandle<Class>> map = Maps.newHashMap();
        for (Field field : classTag.getFields()) {
            String name = field.getName();
            JDBCField annotation = field.getAnnotation(JDBCField.class);
            String description = annotation.description();
            map.put(name, new AnnonationHandle(field, description, field.getType()));
        }
        for (Map.Entry<String, AnnonationHandle<Class>> stringAnnonationHandleEntry : map.entrySet()) {

            System.out.println(stringAnnonationHandleEntry);
        }
        return map;
    }


    public static <T> T composeResult( HashMap<String, AnnonationHandle<Class>> map, ResultSet resultset, Class<T> classType) {

        T student = null;

        try {
            student = classType.newInstance();
            for (Map.Entry<String, AnnonationHandle<Class>> entry : map.entrySet()) {
                Object object = resultset.getObject(entry.getValue().annonationName);
                entry.getValue().field.set(student, object);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return student;
    }


}
