package com.example.springbatchintegrationsample.integration;

import lombok.Data;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.mail.dsl.Mail;
import org.springframework.integration.mail.transformer.AbstractMailMessageTransformer;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;

import javax.mail.Message;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class EmailIntegrationConfig {
    private static final String IMAP_URL = "imaps://[username]:[password]@[host]/[mailbox]";

    @Bean
    @Transformer(inputChannel = "orderInChannel")
    public AbstractMailMessageTransformer mailMessageTransformer() {
        return new AbstractMailMessageTransformer<Order>() {
            @Override
            protected AbstractIntegrationMessageBuilder<Order> doTransform(final Message mailMessage) throws Exception {
                return null;
            }
        };
    }

    @Bean
    public GenericHandler<Order> orderSubmitHandler() {
        return (payload, headers) -> null;
    }

    // @Bean
    public IntegrationFlow orderEmailFlow(final AbstractMailMessageTransformer mailMessageTransformer,
                                          final GenericHandler<Order> orderSubmitHandler) {

        return IntegrationFlows
                .from(Mail.imapInboundAdapter(IMAP_URL),
                        e -> e.poller(Pollers.fixedDelay(10000)))
                .transform(mailMessageTransformer)
                .handle(orderSubmitHandler)
                .get();
    }


    @Data
    public static class Order {
        private final String email;
        private List<Object> tacos = new ArrayList<>();

        public void addTaco(Object taco) {
            this.tacos.add(taco);
        }
    }
}