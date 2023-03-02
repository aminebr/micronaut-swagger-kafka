package my.openapi.app.dto;

import java.io.Serializable;

public class KafkaMessageRecord implements Serializable {

    Integer partition;
    Long offset ;
    Long key;
    String value ;

    public KafkaMessageRecord(Integer partition, Long offset, Long key, String value) {
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public KafkaMessageRecord() {
    }


    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return "KafkaMessageRecord{" +
                "partition=" + partition +
                ", offset=" + offset +
                ", key=" + key +
                ", value='" + value + '\'' +
                '}';
    }
}
