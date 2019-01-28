package com.lkf.v2


import java.text.SimpleDateFormat

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCredential}
import com.mongodb.util.JSON
import com.mongodb.{DBCollection, DBObject, ServerAddress}

import scala.language.postfixOps

object MongodbDemo {

  def main(args: Array[String]): Unit = {


    val mongoClient: MongoClient = createDatabase("192.168.12.59", 27017, "admin", "root", "password")
    mongoClient.dbNames().toList.foreach(x => println(x))
    val collection: DBCollection = mongoClient.getDB("lb").getCollection("lkf_scala_mongo_test")


    //    var collection= createDatabase("localhost", 27017, "mytest", "user", "123456").getCollection("user")
//    for (i <- 1 to 100)
//      collection.insert(MongoDBObject("name" -> "Jack%d".format(i), "email" -> "jack%d@sina.com".format(i), "age" -> i % 25, "birthDay" -> new SimpleDateFormat("yyyy-MM-dd").parse("2016-03-25")))
    import com.google.gson.Gson
    val gson = new Gson
    var person = new Person()
    person.name = "kaifeng"
    person.age = 20
    //转换成json字符串，再转换成DBObject对象
    val dbObject: DBObject = JSON.parse(gson.toJson(person)).asInstanceOf[DBObject]

    collection.insert(dbObject)
  }


  //验证连接权限
  def createDatabase(url: String, port: Int, dbName: String, loginName: String, password: String): MongoClient = {
    var server = new ServerAddress(url, port)
    //注意：MongoCredential中有6种创建连接方式，这里使用MONGODB_CR机制进行连接。如果选择错误则会发生权限验证失败
    var credentials = MongoCredential.createScramSha1Credential(loginName, dbName, password.toCharArray)
    var mongoClient = MongoClient(server, List(credentials))
    mongoClient
  }

}

class Person {
  var name: String = ""
  var age: Int = 0
}
