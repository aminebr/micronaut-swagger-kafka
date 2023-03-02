package my.openapi.app.controllers;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import my.openapi.app.dto.KafkaMessageRecord;
import my.openapi.app.kafkaUtils.KafkaProducerUtil;
import my.openapi.app.dto.TopicPartitionOffsetInfo;
import org.apache.kafka.common.PartitionInfo;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller("/kafka")
public class KafkaControllers {
    /**
     * @return message is sent
     */

    @Get(uri = "/sendSampleMessage", produces = MediaType.TEXT_PLAIN)
    public Mono<String> sendSampleMessage(@QueryValue("message") String message,@QueryValue("topic") String topic ) {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        kafkaProducerUtil.sendMessage(
                "localhost:9092",
                topic,
                message);
        return Mono.just("done");
    }

    @Get(uri = "/listTopics", produces = MediaType.TEXT_PLAIN)
    public Set<String> listTopics() {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        Map<String, List<PartitionInfo>> topics = kafkaProducerUtil.listTopics("localhost:9092");
        return topics.keySet() ;
    }


    @Get(uri = "/consumeMessage", produces = MediaType.TEXT_PLAIN)
    public List<KafkaMessageRecord> consumeMessage(@QueryValue("topic") String topic,
                                                        @QueryValue("partition") Integer partition,
                                                        @QueryValue("offset") Long offset,
                                                        @QueryValue("maxPoll") Integer maxPoll) {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        List<KafkaMessageRecord> messageRecords =
                kafkaProducerUtil.consumeMessage(
                        "localhost:9092",
                        topic,
                        partition,
                        offset,
                        maxPoll);
        return messageRecords ;

    }

    @Get(uri = "/TopicPartitionOffsetDescriptions" ,produces = MediaType.TEXT_PLAIN)
    public List<TopicPartitionOffsetInfo> TopicPartitionDescriptions(@QueryValue("topic") String topic) {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        List<TopicPartitionOffsetInfo> TopicPartitionOffsetInfoList =
                kafkaProducerUtil.TopicPartitionOffsetDescriptions(
                        "localhost:9092",
                        topic);

        return TopicPartitionOffsetInfoList ;

    }



    @Get(uri = "/consumeLastMessage", produces = MediaType.TEXT_PLAIN)
    public List<KafkaMessageRecord> consumeLastMessage(@QueryValue("topic") String topic) {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        List<KafkaMessageRecord> messageRecords =
                kafkaProducerUtil.consumeLastMessage(
                        "localhost:9092",
                        topic);
        return messageRecords ;

    }
}
