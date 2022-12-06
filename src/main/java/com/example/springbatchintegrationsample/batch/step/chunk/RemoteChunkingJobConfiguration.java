package com.example.springbatchintegrationsample.batch.step.chunk;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.*;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.dsl.AmqpInboundChannelAdapterSMLCSpec;
import org.springframework.integration.amqp.dsl.AmqpOutboundChannelAdapterSpec;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageHandlerSpec;
import org.springframework.integration.dsl.MessageProducerSpec;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * 远程分块: Chunking.
 */
@EnableBatchProcessing
@EnableBatchIntegration
@Configuration
public class RemoteChunkingJobConfiguration {

    /**
     * Manager.
     */
    @Slf4j
    @Configuration
    public static class RemoteChunkingManagerConfiguration {
        @Value("${spring.rabbitmq.queue.chunk-requests}")
        private String queueChunkRequests;
        @Value("${spring.rabbitmq.queue.chunk-replies}")
        private String queueChunkReplies;

        /**
         * 用于配置管理器步骤.
         */
        private final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory;

        private final JobBuilderFactory jobBuilderFactory;

        private final JobExecutionListener myJobExecutionListener;

        private final StepExecutionListener myStepExecutionListener;

        private final ChunkListener myChunkListener;

        private final ItemReadListener<Integer> myItemReadListener;

        private final ItemProcessListener<Integer, Integer> myItemProcessListener;

        private final ItemWriteListener<Integer> myItemWriteListener;

        @Autowired
        public RemoteChunkingManagerConfiguration(final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
                                                  final JobBuilderFactory jobBuilderFactory,
                                                  final JobExecutionListener myJobExecutionListener,
                                                  final StepExecutionListener myStepExecutionListener,
                                                  final ChunkListener myChunkListener,
                                                  final ItemReadListener<Integer> myItemReadListener,
                                                  final ItemProcessListener<Integer, Integer> myItemProcessListener,
                                                  final ItemWriteListener<Integer> myItemWriteListener) {
            this.remoteChunkingManagerStepBuilderFactory = remoteChunkingManagerStepBuilderFactory;
            this.jobBuilderFactory = jobBuilderFactory;
            this.myJobExecutionListener = myJobExecutionListener;
            this.myStepExecutionListener = myStepExecutionListener;
            this.myChunkListener = myChunkListener;
            this.myItemReadListener = myItemReadListener;
            this.myItemProcessListener = myItemProcessListener;
            this.myItemWriteListener = myItemWriteListener;
        }


        /**
         * Job.
         */
        @Bean
        public Job remoteChunkingJob() {
            return jobBuilderFactory.get("remoteChunkingJob")
                    .start(remoteChunkManagerStep())
                    .listener(myJobExecutionListener)
                    .build();
        }

        /**
         * Manager Step.
         */
        @Bean
        public TaskletStep remoteChunkManagerStep() {
            final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            final ListItemReader<Integer> itemReader = new ListItemReader<>(source);

            return this.remoteChunkingManagerStepBuilderFactory.get("remoteChunkingManagerStep")
                    .listener(myStepExecutionListener)
                    .chunk(5)
                    .listener(myChunkListener)
                    .outputChannel(remoteChunkingManagerOutgoingRequestToWorkers())
                    .inputChannel(remoteChunkingManagerIncomingRepliesFromWorkers())
                    .reader(itemReader)
                    .listener(myItemReadListener)
                    .listener(myItemProcessListener)
                    .listener(myItemWriteListener)
                    .build();
        }

        /**
         * Outbound IntegrationFlow. (Service Activator's DSL)
         *
         * <p>[inboundAdapter] -> IntegrationFlow -> channel</p>
         *
         * @implSpec <p><b>{@code @Bean}</b> for Register</p>
         */
        @Bean
        public IntegrationFlow remoteChunkingManagerOutboundFlow(
                @Qualifier("rabbitTemplate") final RabbitTemplate rabbitTemplate) {
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(queueChunkRequests);

            return IntegrationFlows
                    .from(remoteChunkingManagerOutgoingRequestToWorkers())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        /**
         * Inbound IntegrationFlow. (Service Activator's DSL)
         *
         * <p>[inboundAdapter] -> IntegrationFlow -> channel</p>
         *
         * @implSpec <p><b>{@code @Bean}</b> for Register</p>
         */
        @Bean
        public IntegrationFlow remoteChunkingManagerInboundFlow(
                @Qualifier("rabbitmqConnectionFactory") final ConnectionFactory rabbitmqConnectionFactory) {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, queueChunkReplies);

            return IntegrationFlows
                    .from(inboundAdapter)
                    .channel(remoteChunkingManagerIncomingRepliesFromWorkers())
                    .get();
        }

        /**
         * Channel: Outgoing.
         * <p>
         * Master's <b>Outgoing Channel</b> -> queueChunkRequests.
         */
        @Bean
        public QueueChannel remoteChunkingManagerOutgoingRequestToWorkers() {
            return new QueueChannel();
        }

        /**
         * Channel: Incoming.
         * <p>
         * Master's <b>Incoming Channel</b> <- QUEUE_REPLIES.
         */
        @Bean
        public QueueChannel remoteChunkingManagerIncomingRepliesFromWorkers() {
            return new QueueChannel();
        }
    }


    /**
     * Worker Configuration.
     * <p>
     * Item Processor.
     * <p>
     * Item Writer.
     */
    @Slf4j
    @Configuration
    public static class RemoteChunkingWorkerConfiguration {
        @Value("${spring.rabbitmq.queue.chunk-requests}")
        private String queueChunkRequests;
        @Value("${spring.rabbitmq.queue.chunk-replies}")
        private String queueChunkReplies;

        /**
         * 用于配置Work步骤.
         */
        private final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder;

        public RemoteChunkingWorkerConfiguration(final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder) {
            this.remoteChunkingWorkerBuilder = remoteChunkingWorkerBuilder;
        }

        /**
         * Work Integration Flow.
         */
        @Bean
        public IntegrationFlow remoteChunkingWorkIntegrationFlow() {
            return this.remoteChunkingWorkerBuilder
                    .inputChannel(remoteChunkingWorkerIncomingRequestsFromManager())
                    .outputChannel(remoteChunkingWorkerOutgoingRepliesToManager())
                    .itemProcessor(processor())
                    .itemWriter(writer())
                    .build();
        }

        /**
         * Channel: Incoming.
         * <p>
         * Worker's <b>Incoming Channel</b> <- queueChunkRequestsS.
         */
        @Bean
        public QueueChannel remoteChunkingWorkerIncomingRequestsFromManager() {
            return new QueueChannel();
        }

        /**
         * Channel: Outgoing.
         * <p>
         * Master's <b>Outgoing Channel</b> -> QUEUE_REPLIES.
         */
        @Bean
        public QueueChannel remoteChunkingWorkerOutgoingRepliesToManager() {
            return new QueueChannel();
        }

        /**
         * Outbound IntegrationFlow. (Service Activator's DSL)
         *
         * <p>[inboundAdapter] -> IntegrationFlow -> channel</p>
         *
         * @implSpec <p><b>{@code @Bean}</b> for Register</p>
         */
        @Bean
        public IntegrationFlow remoteChunkingWorkerInboundFlow(
                @Qualifier("rabbitmqConnectionFactory") final ConnectionFactory rabbitmqConnectionFactory) {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, queueChunkRequests);
            return IntegrationFlows.from(inboundAdapter)
                    .channel(remoteChunkingWorkerIncomingRequestsFromManager())
                    .get();
        }

        /**
         * Outbound IntegrationFlow. (Service Activator's DSL)
         *
         * <p>[inboundAdapter] -> IntegrationFlow -> channel</p>
         *
         * @implSpec <p><b>{@code @Bean}</b> for Register</p>
         */
        @Bean
        public IntegrationFlow remoteChunkingWorkerOutboundFlow(@Qualifier("rabbitTemplate") final RabbitTemplate rabbitTemplate) {
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(queueChunkReplies);

            return IntegrationFlows.from(remoteChunkingWorkerOutgoingRepliesToManager())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        public ItemProcessor<Integer, Integer> processor() {
            return (item) -> {
                log.info("process item: {}, worker: {}", item, InetAddress.getLocalHost().getHostAddress());
                return item;
            };
        }

        public ItemWriter<Integer> writer() {
            return items -> {
                for (final Integer item : items) {
                    log.info("write item: {}, worker: {}", item, InetAddress.getLocalHost().getHostAddress());
                }
            };
        }
    }
}