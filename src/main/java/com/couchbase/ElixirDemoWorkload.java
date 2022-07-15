package com.couchbase;

import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.tracing.opentelemetry.OpenTelemetryRequestTracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class ElixirDemoWorkload {

    public static final String SCOPE_NAME = "sample";
    public static final String COLLECTION_NAME = "first_collection";

    static String connectionString = "127.0.0.1";
    static String username = "Administrator";
    static String password = "password";
    static String [] bucketNames = new String [] { "tenant-1", "tenant-2", "tenant-3"};

    static boolean debug = false;

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        SdkTracerProviderBuilder builder = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn());

        builder.addSpanProcessor(BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder()
                .setEndpoint("http://localhost:4317")
                .build()).build());
        SdkTracerProvider sdkTracerProvider = builder.build();

        OpenTelemetry openTelemetrySdk = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        RequestTracer requestTracer = OpenTelemetryRequestTracer.wrap(openTelemetrySdk);

        ClusterEnvironment clusterEnv =
                ClusterEnvironment.builder()
                        .requestTracer(requestTracer)
                        .build();
        if (debug) System.err.println(clusterEnv);

        // support running against the default ports or the cluster_run ports
        SeedNode seedNode = SeedNode.create(connectionString, Optional.of(12000), Optional.of(9000));
        ClusterOptions clusterOptions =
                ClusterOptions.clusterOptions(username, password).environment(clusterEnv);

        // connect to cluster - with retries
        int retries = 5;
        Cluster cluster = null;
        while (retries-- > 0) {
            cluster = Cluster.connect(Collections.singleton(seedNode), clusterOptions);
            cluster.waitUntilReady(Duration.ofSeconds(30));
            if (cluster.core().clusterConfig().globalConfig() == null) {
                cluster.disconnect();
            } else {
                break;
            }
        }

        runWorkload(cluster);
        sleep(1000);
    }

    private static void runWorkload(Cluster cluster) throws InterruptedException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        final int records = 1000;
        final int tenants = 3;
        final int loops = 300;
        final int concurrency = 20;

        Map<String,String> data = generateData(mapper, "pre1", records);
        loadData(cluster, data, tenants, loops, concurrency);
        sleep(1000);

        //(new Thread(() -> {
        //    readData(records, "pre2", collection);
        //})).start();
        //readData(records, "pre1", collection);
        //sleep(1000);

        queryData(cluster);
        sleep(1000);
        cluster.disconnect();
    }

    private static Map<String,String> generateData(ObjectMapper mapper, String prefix, int records) throws JsonProcessingException {
        System.out.print("Generating " + records + " records of data ...");
        Map<String,String> data = new HashMap<>();

        for (int cnt = 0; cnt < records; ++cnt) {
            ObjectNode jsonValue = createObjectNode(mapper, prefix, 2, 10, 1);
            data.put(cntToKey(prefix, cnt), mapper.writer().writeValueAsString(jsonValue));
        }
        return data;
    }

    private static long loadData(Cluster cluster, Map<String, String> data, int tenants, int loops, int concurrency) throws InterruptedException {

        long start = System.currentTimeMillis();
        System.out.print("Loading data ...");

        IntStream.range(0, tenants).parallel().forEach(tenant -> {
            try {
                Bucket bucket = cluster.bucket(bucketNames[tenant]);
                bucket.waitUntilReady(Duration.parse("PT10S"));
                Collection collection = bucket.scope(SCOPE_NAME).collection(COLLECTION_NAME);
                Semaphore semaphore = new Semaphore(concurrency);
                AsyncCollection async = collection.async();
                for (int i = 0; i < loops; i++) {
                    for (Map.Entry<String, String> entry : data.entrySet()) {
                        semaphore.acquire();
                        async.upsert(entry.getKey(), entry.getValue()).whenComplete((r, e) -> {
                            try {
                                if (e != null) {
                                    e.printStackTrace();
                                }
                            } finally {
                                semaphore.release();
                            }
                        });
                    }
                }
                semaphore.acquire(concurrency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        long loaded = System.currentTimeMillis();
        long loadTime = (loaded - start) / 1000;
        System.out.println("done. " + loops + " loops upserting " + data.size() + " records into " + tenants + " tenants (" + loadTime + "s)");
        return loaded;
    }

    private static void readData(int records, String prefix, Collection collection) {
        long start = System.currentTimeMillis();
        System.out.print("Reading data ... ");

        for (int cnt = 0; cnt < records; ++cnt) {
            GetResult getResult = collection.get(cntToKey(prefix, cnt));
            getResult.contentAsObject().getString("name");
        }

        long read = System.currentTimeMillis();
        long readTime = (read - start) / 1000;
        System.out.println("done. " + records + " records read (" + readTime + "s)");
    }


    private static void queryData(Cluster cluster) throws InterruptedException {
        // Create an index on a collection
        QueryResult result = cluster.query("CREATE PRIMARY INDEX IF NOT EXISTS ON `some_bucket`.`sample`.`first_collection`");

        sleep(1000);
        System.out.println("primary index created");

        String query = "select value v from some_bucket.sample.first_collection v limit 100";
        // Call the query() method on the cluster object and store the result.
        System.out.println("evaluate query \"" + query + "\"");
        result = cluster.query(query);
        System.out.println("returned " + result.rowsAsObject().size() + " records");

        // Return the result rows with the rowsAsObject() method and print to the terminal.
        System.out.println("Query Result:\n" + result.rowsAsObject());
    }

    private static String cntToKey(String prefix, int i) {
        return prefix + "-key-" + i;
    }

    private static ObjectNode createObjectNode(ObjectMapper mapper, String prefix, int width, int length, int depth) {
        if (depth == 0) {
            ObjectNode node = mapper.createObjectNode();
            for (int i = 0; i < width; ++i) {
                node.set("field-" + i, new TextNode(RandomStringUtils.randomAlphabetic(length)));
            }
            return node;
        } else {
            ObjectNode parent = mapper.createObjectNode();
            ObjectNode child = createObjectNode(mapper, prefix, width, length, depth - 1);
            for (int i = 0; i < width; ++i) {
                parent.set(prefix + "-" + "field-" + i, child);
            }
            return parent;
        }
    }
}
