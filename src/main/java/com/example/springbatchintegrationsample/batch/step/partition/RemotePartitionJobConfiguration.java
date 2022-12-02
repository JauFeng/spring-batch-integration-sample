package com.example.springbatchintegrationsample.batch.step.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
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

import java.util.Arrays;
import java.util.List;

/**
 * 远程分区: Partitioning.
 */
@EnableBatchProcessing
@EnableBatchIntegration
@Configuration
public class RemotePartitionJobConfiguration {

    /**
     * Manager.
     */
    @Slf4j
    @Configuration
    public static class RemotePartitionManagerConfiguration {
        @Value("${spring.rabbitmq.queue.partition-requests}")
        private String queuePartitionRequests;
        @Value("${spring.rabbitmq.queue.partition-replies}")
        private String queuePartitionReplies;

        /**
         * 用于配置管理器步骤.
         */
        private final RemotePartitioningManagerStepBuilderFactory remotePartitioningManagerStepBuilderFactory;

        private final JobBuilderFactory jobBuilderFactory;

        @Autowired
        public RemotePartitionManagerConfiguration(final RemotePartitioningManagerStepBuilderFactory remotePartitioningManagerStepBuilderFactory,
                                                   final JobBuilderFactory jobBuilderFactory) {
            this.remotePartitioningManagerStepBuilderFactory = remotePartitioningManagerStepBuilderFactory;
            this.jobBuilderFactory = jobBuilderFactory;
        }

        /**
         * Job.
         */
        @Bean
        public Job remotePartitioningJob(
                @Qualifier("myJobExecutionListener") final JobExecutionListener jobExecutionListener,
                @Qualifier("remotePartitioningManagerStep") final Step remotePartitioningManagerStep) {
            return jobBuilderFactory.get("remotePartitioningJob")
                    .start(remotePartitioningManagerStep)
                    .listener(jobExecutionListener)
                    .build();
        }

        /**
         * Manager Step.
         */
        @Bean
        public Step remotePartitioningManagerStep(
                @Qualifier("myStepExecutionListener") final StepExecutionListener stepExecutionListener) {
            return this.remotePartitioningManagerStepBuilderFactory.get("remotePartitioningManagerStep")
                    .listener(stepExecutionListener)
                    .partitioner("workerStep", new SimplePartitioner()) // 分区
                    .gridSize(5)    // 分区大小
                    .pollInterval(1000) // 轮询时间
                    .outputChannel(remotePartitioningManagerOutgoingRequestToWorkers())
                    .inputChannel(remotePartitioningManagerIncomingRepliesFromWorkers())
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
        public IntegrationFlow remotePartitioningManagerOutboundFlow(
                @Qualifier("rabbitTemplate") final RabbitTemplate rabbitTemplate) {
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(queuePartitionRequests);

            return IntegrationFlows.from(remotePartitioningManagerOutgoingRequestToWorkers())
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
        public IntegrationFlow remotePartitioningManagerInboundFlow(
                @Qualifier("rabbitmqConnectionFactory") final ConnectionFactory rabbitmqConnectionFactory) {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, queuePartitionReplies);

            return IntegrationFlows
                    .from(inboundAdapter)
                    .channel(remotePartitioningManagerIncomingRepliesFromWorkers())
                    .get();
        }

        /**
         * Channel: Outgoing.
         * <p>
         * Master's <b>Outgoing Channel</b> -> queuePartitionRequests.
         */
        @Bean
        public QueueChannel remotePartitioningManagerOutgoingRequestToWorkers() {
            return new QueueChannel();
        }

        /**
         * Channel: Incoming.
         * <p>
         * Master's <b>Incoming Channel</b> <- QUEUE_REPLIES.
         */
        @Bean
        public QueueChannel remotePartitioningManagerIncomingRepliesFromWorkers() {
            return new QueueChannel();
        }
    }


    /**
     * Worker.
     *
     * <p>Step.</p>
     * <p>Item Reader.</p>
     * <p> Item Processor.</p>
     * <p> Item Writer.</p>
     */
    @Slf4j
    @Configuration
    public static class RemotePartitionWorkerConfiguration {
        @Value("${spring.rabbitmq.queue.partition-requests}")
        private String queuePartitionRequests;
        @Value("${spring.rabbitmq.queue.partition-replies}")
        private String queuePartitionReplies;

        private final ApplicationContext applicationContext;
        /**
         * 用于配置Work步骤.
         */
        private final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory;

        public RemotePartitionWorkerConfiguration(final ApplicationContext applicationContext,
                                                  final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory) {
            this.applicationContext = applicationContext;
            this.remotePartitioningWorkerStepBuilderFactory = remotePartitioningWorkerStepBuilderFactory;
        }

        /**
         * Step.
         */
        @Bean
        public TaskletStep workStep(
                @Qualifier("myStepExecutionListener") final StepExecutionListener stepExecutionListener,
                @Qualifier("myChunkListener") final ChunkListener chunkListener) {
            return this.remotePartitioningWorkerStepBuilderFactory.get("remotePartitioningWorkStep")
                    .listener(stepExecutionListener)
                    .inputChannel(remotePartitioningWorkerIncomingRequestsFromManager())
                    .outputChannel(remotePartitioningWorkerOutgoingRepliesToManager())
                    .tasklet(tasklet(null))
                    .listener(chunkListener)
                    .build();
        }

        /**
         * Tasklet.
         */
        @Bean
        @StepScope
        public Tasklet tasklet(@Value("#{stepExecutionContext['partition']") final String partition) {
            return (contribution, chunkContext) -> {
                log.info("processing partition: {}", partition);
                return RepeatStatus.FINISHED;
            };
        }

        /**
         * Channel: Incoming.
         * <p>
         * Worker's <b>Incoming Channel</b> <- queuePartitionRequestsS.
         */
        @Bean
        public QueueChannel remotePartitioningWorkerIncomingRequestsFromManager() {
            return new QueueChannel();
        }

        /**
         * Channel: Outgoing.
         * <p>
         * Master's <b>Outgoing Channel</b> -> QUEUE_REPLIES.
         */
        @Bean
        public QueueChannel remotePartitioningWorkerOutgoingRepliesToManager() {
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
        public IntegrationFlow remotePartitioningWorkerInboundFlow(
                @Qualifier("rabbitmqConnectionFactory") final ConnectionFactory rabbitmqConnectionFactory) {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, queuePartitionRequests);
            return IntegrationFlows.from(inboundAdapter)
                    .channel(remotePartitioningWorkerIncomingRequestsFromManager())
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
        public IntegrationFlow remotePartitioningWorkerOutboundFlow(@Qualifier("rabbitTemplate") final RabbitTemplate rabbitTemplate) {
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(queuePartitionReplies);

            return IntegrationFlows.from(remotePartitioningWorkerOutgoingRepliesToManager())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        public ItemReader<Integer> reader() {
            final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            return new ListItemReader<>(source);
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