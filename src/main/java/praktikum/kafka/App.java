package praktikum.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP", "localhost:9094,localhost:9095,localhost:9096");
        String topic = System.getenv().getOrDefault("TOPIC", "MyTopic");
        String instanceId = System.getenv().getOrDefault("HOSTNAME",
                "app-" + ProcessHandle.current().pid());

        log.info("Starting app instance={} bootstrap={} topic={}",
                instanceId, bootstrap, topic);

        Thread producerThread = new Thread(new ProducerRunner(bootstrap, topic, instanceId), "producer");
        Thread singleThread   = new Thread(new SingleMessageConsumer(bootstrap, topic, instanceId), "single-consumer");
        Thread batchThread    = new Thread(new BatchMessageConsumer(bootstrap, topic, instanceId), "batch-consumer");

        producerThread.start();
        singleThread.start();
        batchThread.start();

        producerThread.join();
        singleThread.join();
        batchThread.join();
    }
}