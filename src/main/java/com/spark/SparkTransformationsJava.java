package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;


/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-10-16 15-31
 */
public class SparkTransformationsJava {
    public static void main( String[] args ) {

    }

    public static void map() {
        SparkConf conf = new SparkConf().setAppName("spark map").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<Integer> listRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaRDD<Integer> numRDD = listRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call( Integer num ) throws Exception {
                return num + 10;
            }
        });
        numRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call( Integer num ) throws Exception {
                System.out.println(num);
            }
        });
    }

    public static void flatmap() {
        SparkConf conf = new SparkConf().setAppName("spark map").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String> listRDD = javaSparkContext
                .parallelize(Arrays.asList("hello wold", "hello java", "hello spark"));
        JavaRDD<String> rdd = listRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call( String input ) throws Exception {
                return Arrays.asList(input.split(" ")).iterator();
            }
        });
        rdd.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call( String num ) throws Exception {
                System.out.println(num);
            }
        });
    }
}
