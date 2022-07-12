package com.couchbase;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.time.Duration;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import org.apache.commons.lang3.RandomStringUtils;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.LoggingMeterConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.metrics.opentelemetry.OpenTelemetryMeter;

import io.opentelemetry.exporter.prometheus.PrometheusCollector;
import io.prometheus.client.exporter.HTTPServer;

public class ElixirDemoWorkload {

    static String connectionString = "couchbase://localhost";
    static String username = "Administrator";
    static String password = "password";
    static String bucketName = "some_bucket";

    public static void main(String[] args) throws IOException, InterruptedException {
        CoreEnvironment coreEnv = CoreEnvironment.builder()
                .loggingMeterConfig(LoggingMeterConfig.enabled(true).emitInterval(Duration.ofSeconds(30))).build();

        // Build the OpenTelemetry Meter
        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder().buildAndRegisterGlobal();
        Meter meter = sdkMeterProvider.get("OpenTelemetryMetricsSample");

        // Start the Prometheus HTTP Server
        HTTPServer server = new HTTPServer(19090);
        // Register the Prometheus Collector
        PrometheusCollector.builder().setMetricProducer(sdkMeterProvider).buildAndRegister();

        ClusterEnvironment clusterEnv =
                ClusterEnvironment.builder().meter(OpenTelemetryMeter.wrap(sdkMeterProvider)).build();
        System.err.println(clusterEnv);
        runWorkload(clusterEnv);

        server.stop();

        sleep(1000);
    }

    private static void runWorkload(ClusterEnvironment clusterEnv) throws InterruptedException {
        ObjectMapper mapper = new ObjectMapper();

        final int records = 100000;

        Cluster cluster = Cluster.connect(connectionString,
                ClusterOptions.clusterOptions(username, password).environment(clusterEnv));

        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.parse("PT10S"));
        Collection collection = bucket.scope("sample").collection("first_collection");

        long loadTime = loadData(mapper, records, collection);

        sleep(1000);
        readData(records, collection, loadTime);

        // Call the query() method on the cluster object and store the result.
        // QueryResult result = cluster.query("select \"Hello World\" as greeting");

        // Return the result rows with the rowsAsObject() method and print to the terminal.
        // System.out.println(result.rowsAsObject());

        sleep(3000);

        cluster.disconnect();
    }

    private static void readData(int records, Collection collection, long loadTime) {
        System.out.print("Reading data ...");

        String name = null;

        for (int cnt = 0; cnt < records; ++cnt) {
            GetResult getResult = collection.get(cntToKey(cnt));
            name = getResult.contentAsObject().getString("name");
        }

        String willi = name;

        long read = System.currentTimeMillis();
        long readTime = (read - loadTime) / 1000;
        System.out.println("done. (" + readTime + "s)");
    }

    private static long loadData(ObjectMapper mapper, int records, Collection collection) {
        long start = System.currentTimeMillis();
        System.out.print("Loading data ...");

        for (int cnt = 0; cnt < records; ++cnt) {
            ObjectNode jsonValue = createObjectNode(mapper, "", 3, 100, 2);
            MutationResult upsertResult = collection.upsert(cntToKey(cnt), jsonValue);
            System.err.println(upsertResult);
        }

        long loaded = System.currentTimeMillis();
        long loadTime = (loaded - start) / 1000;
        System.out.println("done. (" + loadTime + "s)");
        return loaded;
    }

    private static String cntToKey(int i) {
        return "key-" + String.valueOf(i);
    }

    private static ObjectNode createObjectNode(ObjectMapper mapper, String prefix, int width, int length, int depth) {
        if (depth == 0) {
            ObjectNode node = mapper.createObjectNode();
            for (int i = 0; i < width; ++i) {
                node.set(prefix + "field-" + i, new TextNode(RandomStringUtils.randomAlphabetic(length)));
            }
            return node;
        } else {
            ObjectNode parent = mapper.createObjectNode();
            ObjectNode child = createObjectNode(mapper, prefix, width, length, depth - 1);
            for (int i = 0; i < width; ++i) {
                parent.set(prefix + "field-" + i, child);
            }
            return parent;
        }
    }
}
