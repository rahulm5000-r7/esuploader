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
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

public class Uploader {
    private static Logger logger = LogManager.getLogger(Uploader.class);
    private static final List<String> PLATFORMS = Arrays.asList("windows", "linux", "apache", "unix");
    private static final List<String> RESULTS = Arrays.asList("1", "2", "3");
    private static final Random random = new Random();

    Semaphore concurrentRequests = new Semaphore(50000);
    private BulkProcessor processor;
    private RestHighLevelClient client;

    public Uploader() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("<<>>")));

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.info("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                BulkResponse response) {
                if (response.hasFailures()) {
                    logger.info("Bulk [{}] executed with failures", executionId);
                } else {
                    logger.info("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
                concurrentRequests.release(request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
                logger.error(failure.getLocalizedMessage() + failure.getMessage());
                concurrentRequests.release(request.numberOfActions());
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
        builder.setBulkActions(1200);
        builder.setBulkSize(new ByteSizeValue(95L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(25);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        processor = builder.build();
    }

    public void start() throws InterruptedException
    {
        for(int orgId = 0; orgId < Settings.ORG_COUNT; orgId++) {
            for(int assetId = 0; assetId < Settings.ASSET_PER_ORG_COUNT; assetId++) {
                String assetIdentifier = "org_id_" + orgId + "_asset_" + assetId;

                for(int policyId = 0; policyId < Settings.POLICY_PER_ASSET_COUNT; policyId++) {
                    concurrentRequests.acquire();

                    Map<String, Object> policy = new HashMap<>();
                    List<Map<String, Object>> results = new LinkedList<>();
                    policy.put("applicable", random.nextBoolean());
                    policy.put("name", "policy " + policyId);
                    policy.put("asset_id", assetIdentifier);
                    policy.put("org_id", orgId);
                    policy.put("platform", PLATFORMS.get(random.nextInt(PLATFORMS.size())));

                    for(int ruleId = 0; ruleId < Settings.RULE_PER_POLICY_COUNT; ruleId++) {
                        Map<String, Object> ruleResult = new HashMap<>();
                        ruleResult.put("check_name", "rule " + ruleId);
                        ruleResult.put("proof", "this is proof for rule " + ruleId);
                        ruleResult.put("result", RESULTS.get(random.nextInt(RESULTS.size())));
                        results.add(ruleResult);
                    }

                    policy.put("results", results);
                    IndexRequest policyRequest = new IndexRequest("assets_policies", "_doc").source(policy);
                    processor.add(policyRequest);
                }
            }
        }
    }
}
