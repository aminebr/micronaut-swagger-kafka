package my.openapi.app.controllers;

import io.micronaut.context.annotation.Value;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import my.openapi.app.dto.KafkaMessageRecord;
import my.openapi.app.kafkaUtils.KafkaConsumerUtil;

import java.util.List;

@Controller("/kafka/consumer")
public class KafkaConsumerController {


    @Value("${kafka.bootstrap.servers}")
    String BOOTSTRAP_SERVERS;



    @Get(uri = "/consumeMessagesFromTopicAndPartition", produces = MediaType.TEXT_PLAIN)
    public List<KafkaMessageRecord> consumeMessagesFromTopicAndPartition(@QueryValue("topic") String topic,
                                                   @QueryValue("partition") Integer partition,
                                                   @QueryValue("offset") Long offset,
                                                   @QueryValue("maxPoll") Integer maxPoll) {
        KafkaConsumerUtil kafkaConsumerUtil = new KafkaConsumerUtil();
        List<KafkaMessageRecord> messageRecords =
                kafkaConsumerUtil.consumeMessage(
                        BOOTSTRAP_SERVERS,
                        topic,
                        partition,
                        offset,
                        maxPoll);
        return messageRecords ;

    }


    @Get(uri = "/consumeLastMessageFromTopic", produces = MediaType.TEXT_PLAIN)
    public List<KafkaMessageRecord> consumeLastMessage(@QueryValue("topic") String topic) {
        KafkaConsumerUtil kafkaConsumerUtil = new KafkaConsumerUtil();
        List<KafkaMessageRecord> messageRecords =
                kafkaConsumerUtil.consumeLastMessage(
                        BOOTSTRAP_SERVERS,
                        topic);
        return messageRecords ;

    }
}
