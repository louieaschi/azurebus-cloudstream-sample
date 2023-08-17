package com.brillio.azurebuscloudstreamsample;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.azure.spring.messaging.AzureHeaders.CHECKPOINTER;

@SpringBootApplication
@RestController
public class AzurebusCloudstreamSampleApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(AzurebusCloudstreamSampleApplication.class);
	private static final Sinks.Many<Message<String>> many = Sinks.many().unicast().onBackpressureBuffer();


	public static void main(String[] args) {
		SpringApplication.run(AzurebusCloudstreamSampleApplication.class, args);
	}
	@Bean
	public Supplier<Flux<Message<String>>> supply() {
		return ()->many.asFlux()
				.doOnNext(m->LOGGER.info("Manually sending message {}", m))
				.doOnError(t->LOGGER.error("Error encountered", t));
	}

	@Bean
	public Consumer<Message<String>> consume() {
		return message->{
			Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(CHECKPOINTER);
			LOGGER.info("New message received: '{}'", message.getPayload());
			checkpointer.success()
					.doOnSuccess(s->LOGGER.info("Message '{}' successfully checkpointed", message.getPayload()))
					.doOnError(e->LOGGER.error("Error found", e))
					.block();
		};
	}

	@PostMapping("/send")
	public void sendToQueue(@RequestBody String msg) {
		LOGGER.info("Going to add message {} to Sinks.Many.", "Hello Word");
		many.emitNext(MessageBuilder.withPayload(msg).build(), Sinks.EmitFailureHandler.FAIL_FAST);
	}
}
