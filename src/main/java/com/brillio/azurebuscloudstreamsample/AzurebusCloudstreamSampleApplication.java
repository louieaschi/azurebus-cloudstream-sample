package com.brillio.azurebuscloudstreamsample;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.SubQueue;
import com.azure.spring.messaging.checkpoint.Checkpointer;
import com.azure.spring.messaging.servicebus.support.ServiceBusMessageHeaders;
import com.microsoft.azure.servicebus.ClientFactory;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.Message;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.springframework.messaging.MessageHandlingException;
import static com.azure.spring.messaging.AzureHeaders.CHECKPOINTER;

@SpringBootApplication
@RestController
public class AzurebusCloudstreamSampleApplication {

	@Value("${spring.cloud.azure.servicebus.connection-string}")
	private String connectionString;
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AzurebusCloudstreamSampleApplication.class);
	private static final Sinks.Many<Message<SampleObject>> many = Sinks.many().unicast().onBackpressureBuffer();

	public static void main(String[] args) {
		SpringApplication.run(AzurebusCloudstreamSampleApplication.class, args);
	}

	@Bean
	public Supplier<Flux<Message<SampleObject>>> supply() {
		return () -> many.asFlux().doOnNext(m -> LOGGER.info("Manually sending message {}", m))
				.doOnError(t -> LOGGER.error("Error encountered", t));
	}

	@Bean
	public Consumer<Message<SampleObject>> consume() {
		return message -> {
			try {
			Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(CHECKPOINTER);
			LOGGER.info("New message received: '{}'", message.getPayload());
			String payload = message.getPayload().name;
			SampleObject originalPayload = message.getPayload();
			SampleObject modifiedPayload = new SampleObject();
			modifiedPayload.setName(originalPayload.getName() + "_Modified"); // Modify as needed

			Message<SampleObject> modifiedMessage = MessageBuilder.withPayload(modifiedPayload)
					.copyHeaders(message.getHeaders()).build();
			
				// modifiedMessage.getPayload().name = "Changed";
				if (payload.length() < 10) {
					LOGGER.warn("Message '{}' is too short. Sending to dead letter queue.", payload);
					
					ServiceBusReceivedMessageContext messageContext = (ServiceBusReceivedMessageContext) message
							.getHeaders().get(ServiceBusMessageHeaders.RECEIVED_MESSAGE_CONTEXT);
					sendToDLQ(messageContext, "Message is too short. Sending to dead letter queue.","Validation Exception");
					
				} else {
					// Process the message
					checkpointer.success().doOnSuccess(
							s -> LOGGER.info("Message '{}' successfully checkpointed", modifiedMessage.getPayload()))
							.doOnError(e -> LOGGER.error("Error found", e)).block();
					// If successful, acknowledge the message
					LOGGER.info("Message '{}' processed successfully", payload);
				}
			}
		catch (Exception ex) {
			ServiceBusReceivedMessageContext messageContext = (ServiceBusReceivedMessageContext) message
					.getHeaders().get(ServiceBusMessageHeaders.RECEIVED_MESSAGE_CONTEXT);
			sendToDLQ(messageContext, ex.getMessage(),"Consume Exception");
			
			}
		};
	}
	
	public void sendToDLQ(ServiceBusReceivedMessageContext messageContext, String deadLetterErrorDescription, String deadLetterReason) {
		DeadLetterOptions dlo = new DeadLetterOptions();
		dlo.setDeadLetterErrorDescription(deadLetterErrorDescription);
		dlo.setDeadLetterReason(deadLetterReason);
		if (messageContext != null) {
			messageContext.deadLetter(dlo);
		}
	}

	@PostMapping("/send")
	public void sendToQueue(@RequestBody SampleObject msg) {
		LOGGER.info("Going to add message {} to Sinks.Many.", "Hello Word");
		many.emitNext(MessageBuilder.withPayload(msg).build(), Sinks.EmitFailureHandler.FAIL_FAST);
	}

//	@ServiceActivator(inputChannel = "testqueue3..errors")
//	public void handleError(ErrorMessage message) throws InterruptedException, ServiceBusException {
//		LOGGER.error("Handling inbound binding error: " + message.getPayload().getCause().getMessage());
//		System.out.println("@ServiceActivator======= " + message.getOriginalMessage().getPayload());
//		//message.
//		//message = MessageBuilder<>.fromMessage(message).setHeader("Exception", "Exception");
//		throw new RuntimeException("intentional071");
//
//	}

	@GetMapping("/getDLQ")
	public ResponseEntity<ArrayList<IMessage>>  getDLQ() throws InterruptedException, ServiceBusException {
		
		ArrayList<IMessage> results = new ArrayList<>();
		IMessageReceiver deadletterReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(
				new ConnectionStringBuilder(connectionString, "testqueue3/$deadletterqueue"), ReceiveMode.PEEKLOCK);
		while (true) {
			IMessage msg = deadletterReceiver.receive(Duration.ofSeconds(2));
			results.add(msg);
			if (msg != null) {
				System.out.printf("\nDeadletter message:\n");
				if (msg.getProperties() != null) {
					System.out.printf("\tToken\n", msg.getLockToken());
					for (String prop : msg.getProperties().keySet()) {
						System.out.printf("\t%s=%s\n", prop, msg.getProperties().get(prop));

						System.out.println("Received Message " + new String(msg.getBody()));
					}
				}
				
				deadletterReceiver.complete(msg.getLockToken());
				
			} else {
				break;
			}
		}
		deadletterReceiver.close();
		return new ResponseEntity<>(results,HttpStatus.OK);
	}
}
