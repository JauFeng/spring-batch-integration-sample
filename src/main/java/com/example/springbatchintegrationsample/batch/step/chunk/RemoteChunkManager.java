package com.example.springbatchintegrationsample.batch.step.chunk;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.dsl.AmqpOutboundChannelAdapterSpec;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageHandlerSpec;
import org.springframework.integration.dsl.MessageProducerSpec;

import java.util.Arrays;
import java.util.List;

/**
 * 远程分块: Manager Step.
 */
@Configuration
public class RemoteChunkManager {

    private final JobBuilderFactory jobBuilderFactory;

    private final RabbitTemplate rabbitTemplate;

    private final ConnectionFactory rabbitmqConnectionFactory;


    private static final String QUEUE_REQUEST = "requests";

    private static final String QUEUE_REPLY = "replies";


    /**
     * 用于配置管理器步骤.
     */
    private final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory;

    @Autowired
    public RemoteChunkManager(final JobBuilderFactory jobBuilderFactory,
                              final RabbitTemplate rabbitTemplate, final ConnectionFactory rabbitmqConnectionFactory, final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitmqConnectionFactory = rabbitmqConnectionFactory;
        this.remoteChunkingManagerStepBuilderFactory = remoteChunkingManagerStepBuilderFactory;
    }

    @Bean
    public Job remoteChunkingJob() {
        return this.jobBuilderFactory.get("remoteChunkingJob").start(remoteChunkManagerStep()).build();
    }

    /**
     * 远程分块.
     */
    @Bean
    public TaskletStep remoteChunkManagerStep() {
        final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5);
        final ListItemReader<Integer> itemReader = new ListItemReader<>(source);

        return this.remoteChunkingManagerStepBuilderFactory.get("remoteChunkManagerStep")
                .chunk(2)
                .reader(itemReader)
                .outputChannel(managerRequests())
                .inputChannel(managerReplies())
                .build();
    }

    /**
     * 发送消息{@code ChunkRequest}: Master -> QUEUE_REQUEST -> Worker
     */
    @Bean
    public IntegrationFlow managerOutboundFlow() {
        final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundAdapter =
                Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REQUEST);

        return IntegrationFlows.from(managerRequests())
                .handle(outboundAdapter)
                .get();
    }

    /**
     * 接收消息: Master <- QUEUE_REPLY <- Worker
     */
    @Bean
    public IntegrationFlow managerInboundFlow() {
        final MessageProducerSpec inboundAdapter = Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REPLY);

        return IntegrationFlows.from(managerReplies())
                .handle(inboundAdapter)
                .get();
    }

    @Bean
    public DirectChannel managerRequests() {
        return new DirectChannel();
    }

    @Bean
    public QueueChannel managerReplies() {
        return new QueueChannel();
    }
}