package com.lkf.v3

import java.io.{IOException, InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author 刘凯峰
 * @date 2019-12-11 09:55
 * @description hdfs 操作工具类
 */
object HdfsUtil {
  /**
   * 从hdfs读取指定文件
   *
   * @param hdfsUrl  hdfs地址
   * @param filePath 文件地址
   **/
  @throws[IOException]
  def readHDFSFile(hdfsUrl: String, filePath: String): InputStream = {
    val conf = new Configuration
    conf.set("fs.defaultFS", hdfsUrl)
    val fileSystem = FileSystem.get(conf)
    val path = new Path(filePath)
    fileSystem.open(path)
  }
}
