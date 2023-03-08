package my.openapi.app.kafkaUtils;

import my.openapi.app.dto.KafkaMessageRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaConsumerUtil {

    public Properties createNewConsumerProperties(String BOOTSTRAP_SERVERS){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }



    public List<KafkaMessageRecord> consumeMessage(String BOOTSTRAP_SERVERS, String topic, Integer partition, Long offset, Integer maxPoll){

        Properties props = createNewConsumerProperties(BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);


        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));
        /***
         * When you call to poll for the first time from a consumer,
         * it finds a group coordinator, joins the group,
         * receives partition assignment and fetches some records from those partitions
         ***/
        consumer.poll(100);

        consumer.seek(new TopicPartition(topic,partition),offset);
        ConsumerRecords<Long, String> consumerRecords = consumer.poll(100);

        Iterator<ConsumerRecord<Long, String>> iterator = consumerRecords.iterator();
        List<KafkaMessageRecord> kafkaMessageRecordList = new ArrayList<>();
        while (iterator.hasNext()){
            ConsumerRecord<Long, String> consumerRecordsItem = iterator.next();
            kafkaMessageRecordList.add(new KafkaMessageRecord(consumerRecordsItem.partition(),
                    consumerRecordsItem.offset(),
                    consumerRecordsItem.key(),
                    consumerRecordsItem.value())
            );
        }

        consumer.close();
        return kafkaMessageRecordList;
    }







    
}
