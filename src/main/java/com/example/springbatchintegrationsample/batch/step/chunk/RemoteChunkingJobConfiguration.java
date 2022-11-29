package com.example.springbatchintegrationsample.batch.step.chunk;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.dsl.AmqpInboundChannelAdapterSMLCSpec;
import org.springframework.integration.amqp.dsl.AmqpOutboundChannelAdapterSpec;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
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
 * 远程分块.
 */
@EnableBatchProcessing
@EnableBatchIntegration
@Configuration
public class RemoteChunkingJobConfiguration {
    private static final String QUEUE_REQUEST = "my-requests";

    private static final String QUEUE_REPLY = "my-replies";


    /**
     * Manager.
     */
    @Configuration
    public static class ManagerConfiguration {
        /**
         * 用于配置管理器步骤.
         */
        private final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory;

        private final JobBuilderFactory jobBuilderFactory;

        private final RabbitTemplate rabbitTemplate;

        private final ConnectionFactory rabbitmqConnectionFactory;

        @Autowired
        public ManagerConfiguration(final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
                                    final JobBuilderFactory jobBuilderFactory,
                                    final RabbitTemplate rabbitTemplate,
                                    final ConnectionFactory rabbitmqConnectionFactory) {
            this.remoteChunkingManagerStepBuilderFactory = remoteChunkingManagerStepBuilderFactory;
            this.jobBuilderFactory = jobBuilderFactory;
            this.rabbitTemplate = rabbitTemplate;
            this.rabbitmqConnectionFactory = rabbitmqConnectionFactory;
        }

        @Bean
        public Job remoteChunkingJob() {
            return jobBuilderFactory.get("remoteChunkingJob").start(remoteChunkManagerStep()).build();
        }

        /**
         * Manager Step.
         */
        @Bean
        public TaskletStep remoteChunkManagerStep() {
            final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
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
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REQUEST);

            return IntegrationFlows.from(managerRequests())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        /**
         * 接收消息: Master <- QUEUE_REPLY <- Worker
         */
        @Bean
        public IntegrationFlow managerInboundFlow() {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REPLY);

            return IntegrationFlows
                    .from(inboundAdapter)
                    .channel(managerReplies())
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


    /**
     * Worker.
     */
    @Configuration
    public static class WorkerConfiguration {

        private final RabbitTemplate rabbitTemplate;

        private final ConnectionFactory rabbitmqConnectionFactory;

        /**
         * 用于配置Work步骤.
         */
        private final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder;

        public WorkerConfiguration(final RabbitTemplate rabbitTemplate,
                                   final ConnectionFactory rabbitmqConnectionFactory,
                                   final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder) {
            this.rabbitTemplate = rabbitTemplate;
            this.rabbitmqConnectionFactory = rabbitmqConnectionFactory;
            this.remoteChunkingWorkerBuilder = remoteChunkingWorkerBuilder;
        }

        @Bean
        public IntegrationFlow workIntegrationFlow() {
            return this.remoteChunkingWorkerBuilder
                    .itemProcessor(processor())
                    .itemWriter(writer())
                    .inputChannel(workerRequests())
                    .outputChannel(workerReplies())
                    .build();
        }

        @Bean
        public DirectChannel workerRequests() {
            return new DirectChannel();
        }

        @Bean
        public IntegrationFlow workerInboundFlow() {
            final MessageProducerSpec inboundAdapter = Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REQUEST);
            return IntegrationFlows.from(inboundAdapter)
                    .channel(workerRequests())
                    .get();
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
                System.out.println("process item: " + item);
                return item;
            };
        }

        @Bean
        public ItemWriter<Integer> writer() {
            return items -> {
                for (final Integer item : items) {
                    System.out.println("write item: " + item);
                }
            };
        }
    }
}