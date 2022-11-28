package com.example.springbatchintegrationsample;

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

    @Value("thread.pool.core-size")
    private int corePoolSize;
    @Value("thread.pool.max-size")
    private int maxPoolSize;
    @Value("thread.pool.keepalive-time-second")
    private long keepAliveTime;

    @Value("host")
    private String host;
    @Value("port")
    private int port;
    @Value("username")
    private String username;
    @Value("password")
    private String password;
    @Value("virtualHost")
    private String virtualHost;


    @Bean("simplePoolExecutor")
    public ThreadPoolExecutor threadPoolExecutor() {
        return new ThreadPoolExecutor(corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Bean
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

    @Bean
    public RabbitTemplate rabbitTemplate(@Qualifier("rabbitmqConnectionFactory") final ConnectionFactory rabbitmqConnectionFactory) {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitmqConnectionFactory);
        rabbitTemplate.setMandatory(true);  // 交换机(Exchange)无法匹配到队列(queue). True: 返回到生产者队列; False: 直接丢弃.
        return rabbitTemplate;
    }
}