package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class RestClientTest67 {

    private static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        // The default cache size is 100 MiB. Change it to 30 MiB.
        builder.setHttpAsyncResponseConsumerFactory( new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    public static void main(String[] args) throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException, Exception {
        Logger logger = Logger.getLogger(RestClientTest67.class);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        int a = 0;
        String getIndex;
        String getId;
        JSONArray jsonArray = new JSONArray();
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        HttpHost[] hosts;
        RestClientBuilder builder1 = RestClient.builder(new HttpHost("192.168.180.221", 5200));
//        RestClientBuilder builder1 = RestClient.builder(new HttpHost("localhost",9200));
        RestHighLevelClient getClient = new RestHighLevelClient(builder1);
//        try {
        final CredentialsProvider credentialsProvider1 = new BasicCredentialsProvider();
        credentialsProvider1.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("dev", "rahasia2020"));
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, (chain, authType) -> true).build();
        RestClientBuilder postBuilder = RestClient.builder(new HttpHost("192.168.107.21", 9200, "https")).setHttpClientConfigCallback(
                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider1).setSSLHostnameVerifier((hostname, session) -> true).setSSLContext(sslContext)
        );
        RestHighLevelClient postclient = new RestHighLevelClient(postBuilder);


        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest("ima-facebook-hot-202010*");
//               SearchRequest searchRequest = new SearchRequest("nasri_twitter_raw");
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = getClient.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
//        GetIndexRequest getIndexRequest = new GetIndexRequest("nasri_twitter_raw");
//        GetIndexResponse getIndexResponse = getClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        BulkRequest bulkRequest = new BulkRequest();
        int q = 0;
        while (searchHits.length != 0) {
            for (SearchHit hit : searchHits) {
                String index = hit.getIndex();
                String id = hit.getId();
                JSONObject jsonObject = new JSONObject(hit.getSourceAsMap());
                bulkRequest.add(new IndexRequest(index).id(id).type("_doc").source(jsonObject));
                q++;
            }
            BulkResponse bulkResponse = postclient.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println("Data TerInsert = "+q+" : \t"+ dtf.format(now));
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = getClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();

        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = postclient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        postclient.close();
        getClient.close();

    }

}
