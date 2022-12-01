package com.example.springbatchintegrationsample.batch.step.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.dsl.AmqpInboundChannelAdapterSMLCSpec;
import org.springframework.integration.amqp.dsl.AmqpOutboundChannelAdapterSpec;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.channel.DirectChannel;
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

    private static final String QUEUE_REQUEST = "test_partition_requests";
    private static final String QUEUE_REPLY = "test_partition_replies";

    /**
     * Manager.
     */
    @Slf4j
    @Configuration
    public static class ManagerConfiguration {
        /**
         * 用于配置管理器步骤.
         */
        private final RemotePartitioningManagerStepBuilderFactory remotePartitioningManagerStepBuilderFactory;

        private final JobBuilderFactory jobBuilderFactory;

        private final RabbitTemplate rabbitTemplate;

        private final ConnectionFactory rabbitmqConnectionFactory;

        @Autowired
        public ManagerConfiguration(final RemotePartitioningManagerStepBuilderFactory remotePartitioningManagerStepBuilderFactory,
                                    final JobBuilderFactory jobBuilderFactory,
                                    final RabbitTemplate rabbitTemplate,
                                    final ConnectionFactory rabbitmqConnectionFactory) {
            this.remotePartitioningManagerStepBuilderFactory = remotePartitioningManagerStepBuilderFactory;
            this.jobBuilderFactory = jobBuilderFactory;
            this.rabbitTemplate = rabbitTemplate;
            this.rabbitmqConnectionFactory = rabbitmqConnectionFactory;
        }

        @Bean
        public Job remotePartitioningJob() {
            return jobBuilderFactory.get("remotePartitioningJob").start(remotePartitioningManagerStep()).build();
        }

        /**
         * Manager Step.
         */
        @Bean
        public Step remotePartitioningManagerStep() {
            return this.remotePartitioningManagerStepBuilderFactory.get("remotePartitioningManagerStep")
                    .partitioner("workerStep", new SimplePartitioner()) // 分区
                    .gridSize(5)    // 分区大小
                    .pollInterval(1000) // 轮询时间
                    .outputChannel(managerOutgoingRequestToWorkers())
                    .inputChannel(managerIncomingRepliesFromWorkers())
                    .build();
        }

        /**
         * 发送消息{@code ChunkRequest}: Master -> QUEUE_REQUEST -> Worker
         */
        public IntegrationFlow managerOutboundFlow() {
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REQUEST);

            return IntegrationFlows.from(managerOutgoingRequestToWorkers())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        /**
         * 接收消息: Master <- QUEUE_REPLY <- Worker
         */
        public IntegrationFlow managerInboundFlow() {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REPLY);

            return IntegrationFlows
                    .from(inboundAdapter)
                    .channel(managerIncomingRepliesFromWorkers())
                    .get();
        }

        public DirectChannel managerOutgoingRequestToWorkers() {
            return new DirectChannel();
        }

        public DirectChannel managerIncomingRepliesFromWorkers() {
            return new DirectChannel();
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
        private final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory;

        public WorkerConfiguration(final ApplicationContext applicationContext,
                                   final RabbitTemplate rabbitTemplate,
                                   final ConnectionFactory rabbitmqConnectionFactory,
                                   final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory) {
            this.applicationContext = applicationContext;
            this.rabbitTemplate = rabbitTemplate;
            this.rabbitmqConnectionFactory = rabbitmqConnectionFactory;
            this.remotePartitioningWorkerStepBuilderFactory = remotePartitioningWorkerStepBuilderFactory;
        }

        @Bean
        public TaskletStep workStep() {
            return this.remotePartitioningWorkerStepBuilderFactory.get("remotePartitioningWorkStep")
                    .inputChannel(workerRequests())
                    .outputChannel(workerReplies())
                    .tasklet(tasklet(null))
                    .build();
        }

        @Bean
        @StepScope
        public Tasklet tasklet(@Value("#{stepExecutionContext['partition']") final String partition) {
            return (contribution, chunkContext) -> {
                log.info("processing partition: {}", partition);
                return RepeatStatus.FINISHED;
            };
        }


        @Bean
        public DirectChannel workerRequests() {
            return new DirectChannel();
        }

        @Bean
        public IntegrationFlow workerInboundFlow() {
            final MessageProducerSpec<AmqpInboundChannelAdapterSMLCSpec, AmqpInboundChannelAdapter> inboundAdapter =
                    Amqp.inboundAdapter(rabbitmqConnectionFactory, QUEUE_REQUEST);
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
            final MessageHandlerSpec<AmqpOutboundChannelAdapterSpec, AmqpOutboundEndpoint> outboundChannelAdapter =
                    Amqp.outboundAdapter(rabbitTemplate).routingKey(QUEUE_REPLY);

            return IntegrationFlows.from(workerReplies())
                    .handle(outboundChannelAdapter)
                    .get();
        }

        @Bean
        public ItemReader<Integer> reader() {
            final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            return new ListItemReader<>(source);
        }

        @Bean
        public ItemProcessor<Integer, Integer> processor() {
            return (item) -> {
                log.info("process item: {}, worker-{}", item, applicationContext.getApplicationName());
                return item;
            };
        }

        @Bean
        public ItemWriter<Integer> writer() {
            return items -> {
                for (final Integer item : items) {
                    log.info("write item: {}, worker-{}", item, applicationContext.getApplicationName());
                }
            };
        }
    }
}