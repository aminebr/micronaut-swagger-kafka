package my.openapi.app.controllers;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.Hidden;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.net.URISyntaxException;

@Hidden
@Controller("/")
public class HiddenControllers {

    @Get(uri = "/", produces = MediaType.TEXT_PLAIN)
    public HttpResponse<String> redirectionToSwaggerUi() throws URISyntaxException {
        URI location = new URI("/swagger-ui/");
        return HttpResponse.redirect(location);
    }
}
