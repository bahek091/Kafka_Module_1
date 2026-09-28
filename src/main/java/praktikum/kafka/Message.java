package praktikum.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.time.Instant;

@Getter
public class Message {
    private final long id;
    private final String payload;
    private final String producerInstance;
    private final long timestamp;

    @JsonCreator
    public Message(@JsonProperty("id") long id,
                   @JsonProperty("payload") String payload,
                   @JsonProperty("producerInstance") String producerInstance,
                   @JsonProperty("timestamp") long timestamp) {
        this.id = id;
        this.payload = payload;
        this.producerInstance = producerInstance;
        this.timestamp = timestamp;
    }

    public Message(long id, String payload, String producerInstance) {
        this(id, payload, producerInstance, Instant.now().toEpochMilli());
    }

    @Override
    public String toString() {
        return "Message{id=" + id +
                ", payload='" + payload + '\'' +
                ", producerInstance='" + producerInstance + '\'' +
                ", ts=" + timestamp + '}';
    }
}