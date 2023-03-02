package my.openapi.app.dto;


import java.io.Serializable;

public class TopicPartitionOffsetInfo implements Serializable {


    String topic;
    Integer partition;
    Long beginningOffset;
    Long endOffset;

    public TopicPartitionOffsetInfo() {
    }


    public TopicPartitionOffsetInfo(String topic, Integer partition, Long beginningOffset, Long endOffset) {
        this.topic = topic;
        this.partition = partition;
        this.beginningOffset = beginningOffset;
        this.endOffset = endOffset;
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

    public Long getBeginningOffset() {
        return beginningOffset;
    }

    public void setBeginningOffset(Long beginningOffset) {
        this.beginningOffset = beginningOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(Long endOffset) {
        this.endOffset = endOffset;
    }


    @Override
    public String toString() {
        return "TopicPartitionOffsetInfo{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", beginningOffset=" + beginningOffset +
                ", endOffset=" + endOffset +
                '}';
    }
}
