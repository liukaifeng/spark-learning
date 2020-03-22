//package com.lkf.cdh;
//
//import com.cloudera.api.swagger.ImpalaQueriesResourceApi;
//import com.cloudera.api.swagger.client.ApiCallback;
//import com.cloudera.api.swagger.client.ApiClient;
//import com.cloudera.api.swagger.client.ApiException;
//import com.cloudera.api.swagger.client.Configuration;
//import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;
//import com.squareup.okhttp.Interceptor;
//import com.squareup.okhttp.Response;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
//public class ListClusters {
//
//    public static void main( String[] args ) throws ApiException {
//        ApiClient apiClient = Configuration.getDefaultApiClient();
//        apiClient.setUsername("admin");
//        apiClient.setPassword("admin");
//        apiClient.setBasePath("http://192.168.4.21:7180/api/v31");
//        apiClient.setDebugging(true);
//        apiClient.setConnectTimeout(30 * 1000);
//
//        ImpalaQueriesResourceApi impalaQueriesResourceApi = new ImpalaQueriesResourceApi(apiClient);
//        impalaQueriesResourceApi.getImpalaQueryAttributes("cluster3","impala2");
//        String clusterName = "cluster3";
//        String serviceName = "impala2";
//        String queryId = "e147edf96e04ec2b%3Af46d6d8000000000";
//        String format = "";
//
//        impalaQueriesResourceApi.getQueryDetails(clusterName,queryId,serviceName,null);
////        impalaQueriesResourceApi.getQueryDetailsAsync(clusterName, queryId, serviceName, null, new ApiCallback<ApiImpalaQueryDetailsResponse>() {
////            @Override
////            public void onFailure( ApiException e, int i, Map<String, List<String>> map ) {
////
////
////            }
////
////            @Override
////            public void onSuccess( ApiImpalaQueryDetailsResponse apiImpalaQueryDetailsResponse, int i, Map<String, List<String>> map ) {
////                System.out.println("onSuccess:" + apiImpalaQueryDetailsResponse.getDetails());
////            }
////
////            @Override
////            public void onUploadProgress( long l, long l1, boolean b ) {
////
////            }
////
////            @Override
////            public void onDownloadProgress( long l, long l1, boolean b ) {
////
////            }
////        });
//    }
//}