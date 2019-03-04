import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class Consumer {
    private static Logger logger = LogManager.getLogger(Uploader.class);

    private static final Random random = new Random();
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    private RestHighLevelClient client;

    public Consumer() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("")));
    }

    public void start() throws IOException, InterruptedException
    {
        for(int x = 0; x < Settings.ORG_COUNT; x++) {
            Runnable worker = new OrgConsumer(x);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        client.close();
    }

    private class OrgConsumer implements Runnable {

        private int orgId;

        public OrgConsumer(int orgId) {
            this.orgId = orgId;
        }

        public void run() {
//            while(true) {
                Random generator=new Random();
                Integer assetId = generator.nextInt(500000);
                Integer policyId = generator.nextInt(10);


                SearchRequest searchRequest = new SearchRequest("assets_policies_2");
                searchRequest.types("_doc");
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                System.out.println("policy id is policy " + policyId);
                System.out.println("asset_id is org_id_" + this.orgId + "_asset_" + assetId);

                sourceBuilder.query(boolQuery().must(termQuery("name", "policy " + policyId))
                    .must(termQuery("asset_id", "org_id_" + this.orgId + "_asset_" + assetId)));


                Long start = System.currentTimeMillis();

                try {
                    SearchResponse searchResponse = client.search(searchRequest);
                    SearchHits hits = searchResponse.getHits();
                    System.out.println("Search response = " + hits.getTotalHits());
                } catch (IOException e) {
                    e.printStackTrace();
                }


                Long end = System.currentTimeMillis();
                System.out.println("Total runtime: " + (end - start) + " milliseconds");

//            }
        }
    }
}
