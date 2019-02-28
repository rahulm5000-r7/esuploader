import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class Uploader {
    private static Logger logger = LogManager.getLogger(Uploader.class);
    private static final List<String> PLATFORMS = Arrays.asList("windows", "linux", "apache", "unix");
    private static final List<String> RESULTS = Arrays.asList("PASS", "FAIL", "NOT_APPLICABLE");
    private static final Random random = new Random();
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    private BulkProcessor processor;
    private RestHighLevelClient client;

    public Uploader() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("search-policy-benchmark-testing-4zte53pc74mptymajtrs4tocw4.us-east-1.es.amazonaws.com")));

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.info("Executing bulk [{}] with {} requests",
                             executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                BulkResponse response) {
                if (response.hasFailures()) {
                    logger.info("Bulk [{}] executed with failures", executionId);
                } else {
                    logger.info("Bulk [{}] completed in {} milliseconds",
                                 executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
                logger.error(failure.getLocalizedMessage() + failure.getMessage());
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
            (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
        builder.setBulkActions(1000);
        builder.setBulkSize(new ByteSizeValue(9L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(25);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        processor = builder.build();
    }

    public void start() throws IOException, InterruptedException
    {
        Long start = System.currentTimeMillis();
        for(int x = 0; x < Settings.ORG_COUNT; x++) {
            Runnable worker = new OrgAssetUploader(x);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        boolean terminated = processor.awaitClose(300L, TimeUnit.SECONDS);
        processor.close();
        System.out.println("\nFinished all threads");
        client.close();
        Long end = System.currentTimeMillis();
        System.out.println("Total runtime: " + (end - start) / 1000 + " seconds");
    }

    private class OrgAssetUploader implements Runnable {

        private int orgId;

        public OrgAssetUploader(int orgId) {
           this.orgId = orgId;
        }

        public void run() {
            for(int assetId = 0; assetId < Settings.ASSET_PER_ORG_COUNT; assetId++) {
                Map<String, Object> jsonMap = new HashMap<>();
                String assetIdentifier = "org_id_" + orgId + "_asset_" + assetId;
                jsonMap.put("org_id", orgId);
                jsonMap.put("asset_id", assetIdentifier);
                jsonMap.put("platform", PLATFORMS.get(random.nextInt(PLATFORMS.size())));
                jsonMap.put("rules_passed", random.nextInt(Settings.RULE_PER_POLICY_COUNT));
                jsonMap.put("total_rules", random.nextInt(Settings.RULE_PER_POLICY_COUNT));
                IndexRequest request = new IndexRequest("assets_2", "_doc").source(jsonMap);
                processor.add(request);

                for(int policyId = 0; policyId < Settings.POLICY_PER_ASSET_COUNT; policyId++) {
                    Map<String, Object> policy = new HashMap<>();
                    List<Map<String, Object>> results = new LinkedList<>();
                    policy.put("applicable", random.nextBoolean());
                    policy.put("name", "policy " + policyId);
                    policy.put("asset_id", assetIdentifier);

                    for(int ruleId = 0; ruleId < Settings.RULE_PER_POLICY_COUNT; ruleId++) {
                        Map<String, Object> ruleResult = new HashMap<>();
                        ruleResult.put("check_name", "rule " + ruleId);
                        ruleResult.put("proof", "this is proof for rule " + ruleId);
                        ruleResult.put("result", RESULTS.get(random.nextInt(RESULTS.size())));
                        results.add(ruleResult);
                    }

                    policy.put("results", results);
                    IndexRequest policyRequest = new IndexRequest("assets_policies_2", "_doc").source(policy);
                    processor.add(policyRequest);
                }
            }
        }
    }
}
