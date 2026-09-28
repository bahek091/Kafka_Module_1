package praktikum.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SingleMessageConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String bootstrap;
    private final String topic;
    private final String instanceId;

    public SingleMessageConsumer(String bootstrap, String topic, String instanceId) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.instanceId = instanceId;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Группа для простого консьюмера, для параллельного чтения сообщений
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-consumer-group");
        // Автокоммит оффсета
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // По одному сообщению за poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("[{}] SingleMessageConsumer started", instanceId);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        handle(r);
                    }
                } catch (Exception e) {
                    log.error("[{}] Error in single consumer loop", instanceId, e);
                }
            }
        } catch (Exception e) {
            log.error("[{}] Fatal single consumer error", instanceId, e);
        }
    }

    private void handle(ConsumerRecord<String, String> r) {
        try {
            Message msg = MAPPER.readValue(r.value(), Message.class);
            log.info("[{}] SINGLE received partition={} offset={} -> {}",
                    instanceId, r.partition(), r.offset(), msg);
        } catch (Exception ex) {
            log.error("[{}] Deserialization error at partition={} offset={} value={}",
                    instanceId, r.partition(), r.offset(), r.value(), ex);
        }
    }
}