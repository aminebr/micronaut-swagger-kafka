package my.openapi.app.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import my.openapi.app.dto.MessageSentMetadata;
import my.openapi.app.kafkaUtils.KafkaProducerUtil;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Mono;

import java.util.List;

@Controller("/kafka/producer")
public class KafkaProducerController {



    @Value("${kafka.bootstrap.servers}")
    String BOOTSTRAP_SERVERS;

    @Get(uri = "/sendMessagesAsync", produces = MediaType.TEXT_PLAIN)
    public Mono<String> sendMessageAsync(@QueryValue("message") String message, @QueryValue("topic") String topic, @Nullable @QueryValue(value = "partition") Integer partition, @QueryValue("numberOfTimes") Integer numberOfTimes ) {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        kafkaProducerUtil.sendMessageAsync(
                BOOTSTRAP_SERVERS,
                topic,
                message,
                partition,
                numberOfTimes);
        return Mono.just("Async Messages sent");
    }


    @Get(uri = "/sendMessagesSync", produces = MediaType.TEXT_PLAIN)
    public List<MessageSentMetadata> sendMessageSync(@QueryValue("message") String message, @QueryValue("topic") String topic, @Nullable @QueryValue("partition") Integer partition,@QueryValue("numberOfTimes") Integer numberOfTimes ) throws Exception {
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil();
        List<MessageSentMetadata> messageSentMetadata = kafkaProducerUtil.sendMessageSync(
                BOOTSTRAP_SERVERS,
                topic,
                message,
                partition,
                numberOfTimes);

        return messageSentMetadata;
    }




}
