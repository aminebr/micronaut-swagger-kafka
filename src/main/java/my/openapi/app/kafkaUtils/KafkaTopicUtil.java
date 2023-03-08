package my.openapi.app.kafkaUtils;

import my.openapi.app.dto.TopicPartitionOffsetInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaTopicUtil {

    public Map<String, List<PartitionInfo>> listTopics(String BOOTSTRAP_SERVERS){
        Map<String, List<PartitionInfo>> topics;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        topics = consumer.listTopics();

        consumer.close();
        return topics;
    }


    public List<TopicPartitionOffsetInfo> TopicPartitionOffsetDescriptions(String BOOTSTRAP_SERVERS, String topic) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));
        consumer.poll(100);
        List<PartitionInfo> patitionInfoList = consumer.partitionsFor(topic);


        List<TopicPartitionOffsetInfo> TopicPartitionOffsetInfoList = new ArrayList<>();

        for (PartitionInfo partitionInfo : patitionInfoList) {
            TopicPartition tempPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            consumer.seekToBeginning(Collections.singleton(tempPartition));
            long beginningOffset = consumer.position(tempPartition);

            consumer.seekToEnd(Collections.singleton(tempPartition));
            long endOffset = consumer.position(tempPartition) -1;

            TopicPartitionOffsetInfoList.add(new TopicPartitionOffsetInfo(partitionInfo.topic(),partitionInfo.partition(), beginningOffset, endOffset));
        }

        consumer.close();
        return TopicPartitionOffsetInfoList ;
    }

}
