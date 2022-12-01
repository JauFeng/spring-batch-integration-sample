package com.example.springbatchintegrationsample;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ApplicationConfiguration {

    @Value("${thread.pool.core-size:8}")
    private int corePoolSize;
    @Value("${thread.pool.max-size:12}")
    private int maxPoolSize;
    @Value("${thread.pool.keepalive-time:60}")
    private long keepAliveTime;

    @Value("${spring.rabbitmq.host}")
    private String host;
    @Value("${spring.rabbitmq.port}")
    private int port;
    @Value("${spring.rabbitmq.username}")
    private String username;
    @Value("${spring.rabbitmq.password}")
    private String password;
    @Value("${spring.rabbitmq.virtual-host}")
    private String virtualHost;

    @Value("${spring.rabbitmq.queue.chunk-requests}")
    private String queueChunkRequests;

    @Bean("simplePoolExecutor")
    public ThreadPoolExecutor threadPoolExecutor() {
        return new ThreadPoolExecutor(corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Bean("rabbitmqConnectionFactory")
    public ConnectionFactory rabbitmqConnectionFactory(@Qualifier("simplePoolExecutor") final ThreadPoolExecutor threadPoolExecutor) {
        final CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);

        connectionFactory.setExecutor(threadPoolExecutor);
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.SIMPLE);
        connectionFactory.setPublisherReturns(false);
        return connectionFactory;
    }

    @Bean("rabbitTemplate")
    public RabbitTemplate rabbitTemplate(@Qualifier("rabbitmqConnectionFactory") final ConnectionFactory rabbitmqConnectionFactory) {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitmqConnectionFactory);
        rabbitTemplate.setMandatory(true);  // 交换机(Exchange)无法匹配到队列(queue). True: 返回到生产者队列; False: 直接丢弃.
        return rabbitTemplate;
    }

    @Bean
    public Queue remoteChunkRequestsQueue() {
        return new Queue(queueChunkRequests, true, false, false);
    }
}