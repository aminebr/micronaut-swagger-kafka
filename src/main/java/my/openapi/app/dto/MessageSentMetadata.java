package my.openapi.app.dto;

import java.io.Serializable;

public class MessageSentMetadata implements Serializable {

    Long offset;
    Long timestamp;
    String topic;
    Integer partition;


    public MessageSentMetadata() {
    }

    public MessageSentMetadata(Long offset, Long timestamp, String topic, Integer partition) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.topic = topic;
        this.partition = partition;
    }


    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }


    @Override
    public String toString() {
        return "MessageSentMetadata{" +
                "offset=" + offset +
                ", timestamp=" + timestamp +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
}
