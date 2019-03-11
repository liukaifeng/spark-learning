package com.lkf.cdh;

import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.SparkRestClient;

/**
 * todo 一句话描述该类的用途
 *
 * @author 刘凯峰
 * @date 2019-03-06 11-40
 */
public class RestSubmit {
    //http://hadoop-slave2:18088/api/v1/applications
    //POST http://hadoop-slave2:18088/v1/submissions/create HTTP/1.1
    public static void main( String[] args ) throws FailedSparkRequestException {
        SparkRestClient sparkRestClient = SparkRestClient.builder()
                .masterHost("hadoop-slave2")
                .masterPort(18088)
                .sparkVersion("2.3.0")
                .build();
        String submissionId = sparkRestClient.prepareJobSubmit()
                .appName("RestJob")
                .appResource("G://hello-world-1.0-SNAPSHOT.jar")
                .mainClass("com.lkf.DeployTest")
                .submit();
    }
}
