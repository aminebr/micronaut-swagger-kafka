package my.openapi.app.controllers;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import reactor.core.publisher.Mono;

@Controller("/")
public class KafkaControllers {
    /**
     * @return The greeting
     */
    @Get(uri = "/kafka/", produces = MediaType.TEXT_PLAIN)
    public Mono<String> index() {
        return Mono.just("kafka");
    }
}
