package com.example.springbatchintegrationsample.batch.step.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
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
import org.springframework.util.Assert;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                    .partitioner("remotePartitionWorkStep", // 分区
                            myRemotePartitionPartitioner())  // Partitioner.
                    .gridSize(5)    // 分区大小
                    .pollInterval(1000) // 轮询时间
                    .outputChannel(remotePartitioningManagerOutgoingRequestToWorkers())
                    .inputChannel(remotePartitioningManagerIncomingRepliesFromWorkers())
                    .build();
        }

        @Bean
        public Partitioner myRemotePartitionPartitioner() {
            List<String> ids = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

            return new MyRemotePartitionPartitioner(ids);
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

        /**
         * 用于配置Work步骤.
         */
        private final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory;

        public RemotePartitionWorkerConfiguration(final RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory) {
            this.remotePartitioningWorkerStepBuilderFactory = remotePartitioningWorkerStepBuilderFactory;
        }

        /**
         * Step.
         */
        @Bean
        public TaskletStep remotePartitionWorkStep(
                @Qualifier("myStepExecutionListener") final StepExecutionListener stepExecutionListener,
                @Qualifier("myChunkListener") final ChunkListener chunkListener) {
            return this.remotePartitioningWorkerStepBuilderFactory.get("remotePartitioningWorkStep")
                    .listener(stepExecutionListener)
                    .inputChannel(remotePartitioningWorkerIncomingRequestsFromManager())
                    .outputChannel(remotePartitioningWorkerOutgoingRepliesToManager())
                    // 1. do read + process + write by self.
                    // .chunk(5)
                    // .reader(reader())
                    // .processor(processor())
                    // .writer(writer())
                    // 2. do tasklet with partition's parameters.
                    .tasklet(remotePartitionTasklet(null))
                    .listener(chunkListener)
                    .build();
        }

        private String s = "fuck";

        /**
         * Tasklet.
         */
        @Bean
        @StepScope
        public Tasklet remotePartitionTasklet(@Value("#{stepExecutionContext[myRemotePartitionPartitioner.SUB_IDS_KEY]}") final String id) {
            return (contribution, chunkContext) -> {
                log.info("processing id: {}", id);
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

        public ItemProcessor<? super Object, ? super Object> processor() {
            return (item) -> {
                log.info("process item: {}, worker: {}", item, InetAddress.getLocalHost().getHostAddress());
                return item;
            };
        }

        public ItemWriter<? super Object> writer() {
            return items -> {
                for (final Object item : items) {
                    log.info("write item: {}, worker: {}", item, InetAddress.getLocalHost().getHostAddress());
                }
            };
        }
    }

    public static class MyRemotePartitionPartitioner implements Partitioner {
        private static final String PARTITION_KEY = "partition";

        private static final String BEGIN_KEY = "begin";
        private static final String END_KEY = "end";

        public static final String SUB_IDS_KEY = "sub_ids";
        public final List<String> ids;


        public MyRemotePartitionPartitioner(final List<String> ids) {
            this.ids = ids;
        }

        @Override
        public Map<String, ExecutionContext> partition(final int gridSize) {
            Assert.notEmpty(ids, "ids must be not empty.");

            final Map<String, ExecutionContext> map = new HashMap<>(gridSize);


            final int size = ids.size() / gridSize + 1;

            final List<List<String>> partitiveIds = partition(this.ids, size);


            ExecutionContext context;
            int i = 0;
            for (final List<String> subIds : partitiveIds) {
                context = new ExecutionContext();

                context.put(SUB_IDS_KEY, subIds);

                map.put(PARTITION_KEY + i, context);

                i++;
            }

            return map;
        }

        /**
         * 分割列表.
         *
         * @param list 列表
         * @param size 每组大小
         * @return {@code List<List<String>>}
         */
        private static List<List<String>> partition(final List<String> list, final int size) {

            final long partitionCount = partitionCount(list.size(), size);

            List<List<String>> splitList =
                    Stream.iterate(0, n -> n + 1)
                            .limit(partitionCount).parallel()
                            .map(a ->
                                    list.stream().skip(a * size)
                                            .limit(size).parallel()
                                            .collect(Collectors.toList()))
                            .collect(Collectors.toList());

            return splitList;
        }

        /**
         * 计算切片.
         */
        private static long partitionCount(final long listSize, final long partitionSize) {
            Assert.isTrue(listSize > 0, "List size must be greater than 0");
            Assert.isTrue(partitionSize > 0, "Partition size must be greater than 0");

            return (listSize + partitionSize - 1) / partitionSize;
        }
    }
}