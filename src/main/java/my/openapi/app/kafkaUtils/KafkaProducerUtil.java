package my.openapi.app.kafkaUtils;


import my.openapi.app.dto.MessageSentMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class KafkaProducerUtil {


    public Properties createNewPropetiesObjectForProducer(String BOOTSTRAP_SERVERS){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    public void sendMessageAsync(String BOOTSTRAP_SERVERS, String topic,String message,Integer partition , Integer numberOfTimes) {

        Properties props = createNewPropetiesObjectForProducer(BOOTSTRAP_SERVERS);


        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0 ; i < numberOfTimes; i++) {
            ProducerRecord<String, String> record;
            if(partition != null ){
                record = new ProducerRecord<>(topic,partition,null, message);
            }
            else {
                record = new ProducerRecord<>(topic, null, message);
            }

            producer.send(record);
        }
        producer.close();

    }



    public List<MessageSentMetadata> sendMessageSync(String BOOTSTRAP_SERVERS, String topic,String message,Integer partition ,  Integer numberOfTimes) throws Exception{

        Properties props = createNewPropetiesObjectForProducer(BOOTSTRAP_SERVERS);

        Producer<String, String> producer = new KafkaProducer<>(props);

        List<MessageSentMetadata> messageSentMetadataList = new ArrayList<>();


        for(int i = 0 ; i < numberOfTimes; i++){

            ProducerRecord<String, String> record;
            if(partition != null){
                record = new ProducerRecord<>(topic, partition, null, message);
            }
            else{
                record = new ProducerRecord<>(topic, null, message);
            }

            RecordMetadata recordMetadata = producer.send(record).get();

            messageSentMetadataList.add(new MessageSentMetadata(recordMetadata.offset(),
                    recordMetadata.timestamp(),
                    recordMetadata.topic(),
                    recordMetadata.partition()));
        }

        producer.close();

        return messageSentMetadataList ;

    }



}
