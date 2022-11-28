package com.example.springbatchintegrationsample.batch.step.chunk;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageHandlerSpec;

/**
 * 远程分块: Worker.
 */
@Configuration
public class RemoteChunkWorker {

    private static final String QUEUE_REQUEST = "requests";

    private static final String QUEUE_REPLY = "replies";

    private final RabbitTemplate rabbitTemplate;

    private final RemoteChunkingWorkerBuilder remoteChunkingWorkerBuilder;

    public RemoteChunkWorker(final RabbitTemplate rabbitTemplate, final RemoteChunkingWorkerBuilder remoteChunkingWorkerBuilder) {
        this.rabbitTemplate = rabbitTemplate;
        this.remoteChunkingWorkerBuilder = remoteChunkingWorkerBuilder;
    }

    @Bean
    public DirectChannel workerReplies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow workerOutboundFlow() {
        final MessageHandlerSpec outboundChannelAdapter = Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REPLY);

        return IntegrationFlows.from(workerReplies())
                .handle(outboundChannelAdapter)
                .get();
    }

    @Bean
    public ItemProcessor<Integer, Integer> processor() {
        return (item) -> {
            System.out.println(item);
            return item;
        };
    }
}