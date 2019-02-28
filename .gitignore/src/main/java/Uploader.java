import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Uploader {
    private static final List<String> PLATFORMS = Arrays.asList("windows", "linux", "apache", "unix");
    private static final List<String> RESULTS = Arrays.asList("PASS", "FAIL", "NOT_APPLICABLE");
    private static final Random random = new Random();
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    private BulkProcessor processor;
    private RestHighLevelClient client;

    public Uploader() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("<<put_host_here>>", 9200, "http")));

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("before bulk");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("after bulk");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("after bulk error");
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
        builder.setBulkActions(500);
        builder.setBulkSize(new ByteSizeValue(9L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(10);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        processor = builder.build();
    }

    public void start() throws IOException {
        // start ORG_COUNT threads and each thread creates ASSET_COUNT index requests
        for(int x = 0; x < Settings.ORG_COUNT; x++) {
            Runnable worker = new OrgAssetUploader(x);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        client.close();
    }

    private class OrgAssetUploader implements Runnable {

        private int orgId;

        public OrgAssetUploader(int orgId) {
           this.orgId = orgId;
        }

        public void run() {
            for(int assetId = 0; assetId < Settings.ASSET_PER_ORG_COUNT; assetId++) {
                Map<String, Object> jsonMap = new HashMap<>();
                List<Map<String, Object>> policies = new LinkedList<>();

                for(int policyId = 0; policyId < Settings.POLICY_PER_ASSET_COUNT; policyId++) {
                    Map<String, Object> policy = new HashMap<>();
                    List<Map<String, Object>> results = new LinkedList<>();

                    policy.put("applicable", random.nextBoolean());
                    policy.put("name", "policy " + policyId);

                    for(int ruleId = 0; ruleId < Settings.RULE_PER_POLICY_COUNT; ruleId++) {
                        Map<String, Object> ruleResult = new HashMap<>();
                        ruleResult.put("check_name", "rule " + ruleId);
                        ruleResult.put("proof", "this is proof for rule " + ruleId);
                        ruleResult.put("result", RESULTS.get(random.nextInt(RESULTS.size())));
                        results.add(ruleResult);
                    }

                    policy.put("results", results);
                    policies.add(policy);
                }

                jsonMap.put("policies", policies);
                jsonMap.put("org_id", orgId);
                jsonMap.put("asset_id", "org_id_" + orgId + "_asset_" + assetId);
                jsonMap.put("platform", PLATFORMS.get(random.nextInt(PLATFORMS.size())));
                jsonMap.put("rules_passed", random.nextInt(Settings.RULE_PER_POLICY_COUNT));
                jsonMap.put("total_rules", random.nextInt(Settings.RULE_PER_POLICY_COUNT));
                IndexRequest request = new IndexRequest("assets", "doc").source(jsonMap);
//                processor.add(request);
                System.out.println(request.sourceAsMap());
            }
        }
    }
}
