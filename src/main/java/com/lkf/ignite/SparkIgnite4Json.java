package com.lkf.ignite;

import org.apache.ignite.spark.IgniteContext;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-07-25 10-37
 */
public class SparkIgnite4Json {
    private static final String CONFIG = "config/default-config.xml";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Ignite data sources write example")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);
        Dataset<Row> personsDataFrame = spark.read().json("/kaifeng/person.json");


        IgniteContext igniteContext=new IgniteContext(spark.sparkContext());

        personsDataFrame.show();

        System.out.println("Writing Data Frame to Ignite:");

        //Writing content of data frame to Ignite.
        /**
         personsDataFrame.write()
         .format(IgniteDataFrameSettings.FORMAT_IGNITE())
         .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
         .option(IgniteDataFrameSettings.OPTION_TABLE(), "json_person")
         .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
         .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
         .mode(SaveMode.Overwrite)
         .save();
         **/
        //Writing content of data frame to Ignite.
        personsDataFrame.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "json_person")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                .mode(SaveMode.Overwrite)
                .save();
        System.out.println("Done!");

        System.out.println("Reading data from Ignite table:");
    }



}
