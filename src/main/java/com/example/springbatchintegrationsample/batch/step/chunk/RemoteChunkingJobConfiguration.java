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
import org.springframework.context.ApplicationContext;
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
 * 远程分块: Chunking.
 */
@EnableBatchProcessing
@EnableBatchIntegration
@Configuration
public class RemoteChunkingJobConfiguration {
    private static final String QUEUE_REQUEST = "test_chunk_requests";

    private static final String QUEUE_REPLY = "test_chunk_replies";

    /**
     * Manager.
     */
    @Slf4j
    @Configuration
    public static class ManagerConfiguration {
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
        public ManagerConfiguration(final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
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
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REQUEST);

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
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REPLY);

            return IntegrationFlows
                    .from(inboundAdapter)
                    .channel(remoteChunkingManagerIncomingRepliesFromWorkers())
                    .get();
        }

        /**
         * Channel: Outgoing.
         * <p>
         * Master -> <b>QUEUE_REQUEST<b/> -> Worker.
         */
        @Bean
        public QueueChannel remoteChunkingManagerOutgoingRequestToWorkers() {
            return new QueueChannel();
        }

        /**
         * Channel: Incoming.
         * <p>
         * Worker -> <b>QUEUE_REPLY<b/> -> Master.
         */
        @Bean
        public QueueChannel remoteChunkingManagerIncomingRepliesFromWorkers() {
            return new QueueChannel();
        }
    }


    /**
     * Worker.
     */
    @Slf4j
    @Configuration
    public static class WorkerConfiguration {

        private final ApplicationContext applicationContext;
        private final RabbitTemplate rabbitTemplate;
        private final ConnectionFactory rabbitmqConnectionFactory;

        /**
         * 用于配置Work步骤.
         */
        private final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder;

        public WorkerConfiguration(final ApplicationContext applicationContext,
                                   final RabbitTemplate rabbitTemplate,
                                   final ConnectionFactory rabbitmqConnectionFactory,
                                   final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder) {
            this.applicationContext = applicationContext;
            this.rabbitTemplate = rabbitTemplate;
            this.rabbitmqConnectionFactory = rabbitmqConnectionFactory;
            this.remoteChunkingWorkerBuilder = remoteChunkingWorkerBuilder;
        }

        @Bean
        public IntegrationFlow workIntegrationFlow() {
            return this.remoteChunkingWorkerBuilder
                    .inputChannel(workerIncomingRequestsFromManager())
                    .outputChannel(workerOutgoingRepliesToManager())
                    .itemProcessor(processor())
                    .itemWriter(writer())
                    .build();
        }

        @Bean
        public DirectChannel workerIncomingRequestsFromManager() {
            return new DirectChannel();
        }

        @Bean
        public IntegrationFlow remoteChunkingWorkerInboundFlow() {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REQUEST);
            return IntegrationFlows.from(inboundAdapter)
                    .channel(workerIncomingRequestsFromManager())
                    .get();
        }

        @Bean
        public DirectChannel workerOutgoingRepliesToManager() {
            return new DirectChannel();
        }

        @Bean
        public IntegrationFlow remoteChunkingWorkerOutboundFlow() {
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REPLY);

            return IntegrationFlows.from(workerOutgoingRepliesToManager())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        public ItemProcessor<Integer, Integer> processor() {
            return (item) -> {
                log.info("process item: {}, worker-{}", item, applicationContext.getApplicationName());
                return item;
            };
        }

        public ItemWriter<Integer> writer() {
            return items -> {
                for (final Integer item : items) {
                    log.info("write item: {}, worker-{}", item, applicationContext.getApplicationName());
                }
            };
        }
    }
}