package praktikum.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BatchMessageConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BatchMessageConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String bootstrap;
    private final String topic;
    private final String instanceId;

    public BatchMessageConsumer(String bootstrap, String topic, String instanceId) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.instanceId = instanceId;
    }

    @Override
    public void run() {
        Properties props = getProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("[{}] BatchMessageConsumer started", instanceId);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                    if (records.isEmpty()) {
                        continue;
                    }

                    log.info("[{}] BATCH polled {} records", instanceId, records.count());

                    for (ConsumerRecord<String, String> r : records) {
                        handle(r);
                    }

                    consumer.commitSync();
                    log.info("[{}] BATCH committed offsets for {} records",
                            instanceId, records.count());

                } catch (Exception e) {
                    log.error("[{}] Error in batch consumer loop", instanceId, e);
                }
            }
        } catch (Exception e) {
            log.error("[{}] Fatal batch consumer error", instanceId, e);
        }
    }

    @NotNull
    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // group_id для пакетной обработки, для параллельного чтения сообщений
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");
        // Ручной коммит.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Минимум 10 сообщений за один poll.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        // fetch.min.bytes — минимальный объём данных, который брокер отдаст за один fetch.
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1500);
        // fetch.max.wait.ms — максимум ожидания накопления fetch.min.bytes.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        return props;
    }

    private void handle(ConsumerRecord<String, String> r) {
        try {
            Message msg = MAPPER.readValue(r.value(), Message.class);
            log.info("[{}] BATCH received partition={} offset={} -> {}",
                    instanceId, r.partition(), r.offset(), msg);
        } catch (Exception ex) {
            log.error("[{}] Deserialization error at partition={} offset={} value={}",
                    instanceId, r.partition(), r.offset(), r.value(), ex);
        }
    }
}

