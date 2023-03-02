package my.openapi.app.kafkaUtils;


import my.openapi.app.dto.KafkaMessageRecord;
import my.openapi.app.dto.TopicPartitionOffsetInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;


public class KafkaProducerUtil {




    public void sendMessage(String BOOTSTRAP_SERVERS, String topic,String message) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer kafkaProducer =  new KafkaProducer<>(props);


        try{
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, null, message );
            RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }


    }

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


    public List<KafkaMessageRecord> consumeMessage(String BOOTSTRAP_SERVERS, String topic, Integer partition, Long offset,Integer maxPoll){

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);


        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));

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

        return kafkaMessageRecordList;
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

        return TopicPartitionOffsetInfoList ;
    }


    public List<KafkaMessageRecord> consumeLastMessage(String BOOTSTRAP_SERVERS, String topic) {


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");



        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));

        consumer.poll(100);
        List<PartitionInfo> patitionInfoList = consumer.partitionsFor(topic);


        List<TopicPartition> TopicPartitionList = new ArrayList<>();

        for (PartitionInfo partitionInfo : patitionInfoList) {
            TopicPartition tempPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            TopicPartitionList.add(tempPartition);
        }

        consumer.seekToEnd(TopicPartitionList);
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

        return kafkaMessageRecordList;
    }
}
