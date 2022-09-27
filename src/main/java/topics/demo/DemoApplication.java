package topics.demo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.*;


@SpringBootApplication
public class DemoApplication {

	@Value("${hostName}")
    private String hostName;

	@Value("${queueName}")
    private String queueName;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}


	@PostConstruct
	public void init() {
		
		CountDownLatch countdownLatch = new CountDownLatch(1);
		ServiceBusProcessorClient client =  new ServiceBusClientBuilder()
		.credential(hostName, new DefaultAzureCredentialBuilder().build())
		.processor()
		.queueName(queueName)
		.processMessage(DemoApplication::processMessage)
		.processError(context -> processError(context, countdownLatch))
		.buildProcessorClient();

		 client.start();
	}


	public static void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage message = context.getMessage();
        System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", message.getMessageId(),
            message.getSequenceNumber(), message.getBody());
    } 


	private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
		System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
			context.getFullyQualifiedNamespace(), context.getEntityPath());
	
		if (!(context.getException() instanceof ServiceBusException)) {
			System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
			return;
		}
	
		ServiceBusException exception = (ServiceBusException) context.getException();
		ServiceBusFailureReason reason = exception.getReason();
	
		if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
			|| reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
			|| reason == ServiceBusFailureReason.UNAUTHORIZED) {
			System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
				reason, exception.getMessage());
	
			countdownLatch.countDown();
		} else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
			System.out.printf("Message lock lost for message: %s%n", context.getException());
		} else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
			try {
				// Choosing an arbitrary amount of time to wait until trying again.
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				System.err.println("Unable to sleep for period of time");
			}
		} else {
			System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(),
				reason, context.getException());
		}
	}

}
