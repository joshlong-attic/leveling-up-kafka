package com.example.basics;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.support.RetryTemplate;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SpringBootApplication
public class BasicsApplication {

    public static void main(String[] args) {
        SpringApplication.run(BasicsApplication.class, args);
    }
}

@Configuration
class StreamConfiguration {

    @Bean
    Consumer<Object> consumer() {
        return message -> System.out.println("new message: " + new String ((byte[]) message));
    }
}

@Profile("integration")
@Configuration
class IntegrationConfiguration {


    @Bean
    IntegrationFlow incomingStringsFlow(MessageChannel stringMessages) {
        return IntegrationFlows
                .from(stringMessages)
                .handle((GenericHandler<String>) (payload, headers) -> {
                    System.out.println("new message: " + payload);
                    headers.forEach((k, v) -> System.out.println(k + ":" + v));
                    return null;
                })
                .get();
    }

    @Bean
    MessageChannel stringMessages() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow kafkaFlow(ConsumerFactory<String, String> cf) {

        return IntegrationFlows
                .from(Kafka.messageDrivenChannelAdapter(cf,
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record, "messages")
                        .configureListenerContainer(c ->
                                c
                                        .groupId("messages")
                                        .ackMode(ContainerProperties.AckMode.MANUAL)
                                        .idleEventInterval(100L)
                                        .id("messages"))
                        .retryTemplate(new RetryTemplate()))
                .channel(stringMessages())
                .get();


    }

    @Bean
    IntegrationFlow filesFlow(ConsumerFactory<String, String> factory) {
        var desktop = new File(System.getenv("HOME"), "Desktop");
        var in = new File(desktop, "in");
        var files = Files
                .inboundAdapter(in)
                .autoCreateDirectory(true)
                .get();
        return IntegrationFlows
                .from(files, poller -> poller.poller(pm -> pm.fixedRate(1, TimeUnit.SECONDS)))
                .transform(new FileToStringTransformer())
                .channel(stringMessages())
                .get();
    }


}

@Profile("basics")
@Configuration
class BasicSpringforApacheKafkaConfiguration {

    @KafkaListener(groupId = "messages", topics = "messages")
    public void handleNewMessage(String message) {
        System.out.println("received: " + message);
    }
}

@Configuration
class ProducerConfiguration {

    @Bean
    ApplicationRunner runner(KafkaTemplate<String, Object> template) {
        return args -> template.send("messages", "Hello, world!!".getBytes(StandardCharsets.UTF_8));
    }
}
