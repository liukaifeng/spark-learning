package com.lkf.ignite

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.spark.sql.SparkSession
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.jute.compiler.{JLong, JString}
/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-07-25 17-58
  */
object SparkSave2Ignite {
  private val CONFIG = "examples/config/example-ignite.xml"

  private val CACHE_NAME = "testCache"

  def setupServerAndData: Ignite = {
    //Starting Ignite.
    val ignite = Ignition.start(CONFIG)

    //Creating first test cache.
    val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

    val cache = ignite.getOrCreateCache(ccfg)

    //Creating SQL table.
    cache.query(new SqlFieldsQuery(
      "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id)) " +
        "WITH \"backups=1\"")).getAll

    cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

    //Inserting some data to tables.
    val qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)")

    cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 3L.asInstanceOf[JLong])).getAll
    cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Jane Roe", 2L.asInstanceOf[JLong])).getAll
    cache.query(qry.setArgs(3L.asInstanceOf[JLong], "Mary Major", 1L.asInstanceOf[JLong])).getAll
    cache.query(qry.setArgs(4L.asInstanceOf[JLong], "Richard Miles", 2L.asInstanceOf[JLong])).getAll

    ignite
  }


}
