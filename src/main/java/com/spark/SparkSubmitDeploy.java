//package com.spark;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.deploy.SparkSubmit;
//import org.apache.spark.deploy.rest.CreateSubmissionResponse;
//import org.apache.spark.deploy.rest.RestSubmissionClient;
//import org.apache.spark.deploy.rest.SubmissionStatusResponse;
//
//import java.util.HashMap;
//
///**
// * todo 一句话描述该类的用途
// *
// * @author 刘凯峰
// * @date 2019-03-05 17-50
// */
//public class SparkSubmitDeploy {
////    public void submit() {
////        String appResource = "G:\\hello-world-1.0-SNAPSHOT.jar";
////        String mainClass = "com.lkf.DeployTest";
////        String[] args = {};
////
////        SparkConf sparkConf = new SparkConf();
////        // 下面的是参考任务实时提交的Debug信息编写的
////        sparkConf.setMaster("spark://192.168.12.201:6066")
////                .setAppName("deploy_test")
////                .set("spark.executor.cores", "1")
////                .set("spark.submit.deployMode", "cluster")
////                .set("spark.jars", appResource)
////                .set("spark.executor.memory", "1g")
////                .set("spark.cores.max", "1")
////                .set("spark.driver.supervise", "false");
////        Map<String,String> confMap= Maps.newHashMap();
////        SubmitRestProtocolResponse response = null;
////
////        try {
////            RestSubmissionClient restSubmissionClient = new RestSubmissionClient("spark://192.168.12.201:6066");
////            CreateSubmissionRequest createSubmissionRequest = restSubmissionClient.constructSubmitRequest(appResource, mainClass, args, (scala.collection.immutable.Map<String, String>) confMap, null);
////            response = restSubmissionClient.createSubmission(createSubmissionRequest);
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
////        System.out.println(response.toJson());
////    }
//
//    public static void main(String[] args) {
//        SparkSubmit.main(args);
//        String id = submit();
//        boolean flag;
//        while (true){
//            flag = monitory(id);
//            if (flag) {
//                break;
//            }
//        }
//        System.out.println("spark执行完成");
//    }
//
//    public static String submit() {
//        String appResource = "D:\\hello-world-1.0-SNAPSHOT.jar";
//        String mainClass = "com.lkf.DeployTest";
//        String[] args = {  };
//
//        SparkConf sparkConf = new SparkConf();
//
//        sparkConf.setMaster("spark://192.168.1.107:6066");
//        sparkConf.set("spark.submit.deployMode", "cluster");
//        sparkConf.set("spark.jars", appResource);
//        sparkConf.set("spark.driver.supervise", "false");
//        sparkConf.setAppName("queryLog"+ System.currentTimeMillis());
//
//        CreateSubmissionResponse response = null;
//
//        try {
//            response = (CreateSubmissionResponse)
//                    RestSubmissionClient.run(appResource, mainClass, args, sparkConf, new HashMap<String,String>());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return response.submissionId();
//    }
//
//    private static RestSubmissionClient client = new RestSubmissionClient("spark://192.168.1.107:6066");
//
//    public static boolean monitory(String appId){
//        SubmissionStatusResponse response = null;
//        boolean finished =false;
//        try {
//            response = (SubmissionStatusResponse) client.requestSubmissionStatus(appId, true);
//            if("FINISHED" .equals(response.driverState()) || "ERROR".equals(response.driverState())){
//                finished = true;
//            }
//            Thread.sleep(5000);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return finished;
//    }
//
//}
