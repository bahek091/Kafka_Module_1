package praktikum.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String bootstrap;
    private final String topic;
    private final String instanceId;

    public ProducerRunner(String bootstrap, String topic, String instanceId) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.instanceId = instanceId;
    }

    @Override
    public void run() {
        Properties props = getProperties();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            AtomicLong counter = new AtomicLong(0);
            while (!Thread.currentThread().isInterrupted()) {
                long id = counter.incrementAndGet();
                Message msg = new Message(id, "payload-" + id, instanceId);

                String json;
                try {
                    json = MAPPER.writeValueAsString(msg);
                } catch (Exception serEx) {
                    log.error("[{}] Serialization error for msg id={}", instanceId, id, serEx);
                    continue;
                }

                // Вывод отправляемого сообщения в консоль.
                log.info("[{}] Sending -> {}", instanceId, json);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, String.valueOf(id), json);

                // Асинхронная отправка (push) + callback.
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Send failed for id={}", instanceId, id, ex);
                    } else {
                        log.info("[{}] Ack id={} partition={} offset={}",
                                instanceId, id,
                                metadata.partition(), metadata.offset());
                    }
                });

                Thread.sleep(500);
            }
            producer.flush();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("[{}] Producer fatal error", instanceId, e);
        }
    }

    @NotNull
    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Гарантии доставки At Least Once
        // acks=all — ждём подтверждения от всех реплик.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // retries — повторные попытки при временных ошибках.
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        // Общий таймаут доставки, чтобы не залипнуть навсегда.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        // Идемпотентность — защита от дублей при ретраях.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        return props;
    }
}