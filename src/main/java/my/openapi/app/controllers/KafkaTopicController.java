package my.openapi.app.controllers;

import io.micronaut.context.annotation.Value;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import my.openapi.app.dto.TopicPartitionOffsetInfo;
import my.openapi.app.kafkaUtils.KafkaTopicUtil;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller("/kafka/topic")
public class KafkaTopicController {



    @Value("${kafka.bootstrap.servers}")
    String BOOTSTRAP_SERVERS;


    @Get(uri = "/listTopics", produces = MediaType.TEXT_PLAIN)
    public Set<String> listTopics() {
        KafkaTopicUtil kafkaTopicUtil = new KafkaTopicUtil();
        Map<String, List<PartitionInfo>> topics = kafkaTopicUtil.listTopics(BOOTSTRAP_SERVERS);
        return topics.keySet() ;
    }


    @Get(uri = "/TopicPartitionOffsetDescriptions" ,produces = MediaType.TEXT_PLAIN)
    public List<TopicPartitionOffsetInfo> TopicPartitionDescriptions(@QueryValue("topic") String topic) {
        KafkaTopicUtil kafkaTopicUtil = new KafkaTopicUtil();
        List<TopicPartitionOffsetInfo> TopicPartitionOffsetInfoList =
                kafkaTopicUtil.TopicPartitionOffsetDescriptions(
                        BOOTSTRAP_SERVERS,
                        topic);

        return TopicPartitionOffsetInfoList ;

    }
}
