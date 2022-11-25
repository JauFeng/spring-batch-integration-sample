package com.example.springbatchintegrationsample.integration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Filter;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.annotation.Splitter;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.router.AbstractMessageRouter;
import org.springframework.integration.selector.PayloadTypeSelector;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Slf4j
@Configuration
public class SimpleIntegrationConfig {

    /**
     * Integration Flow.
     * <p>
     * text -> file.
     *
     * @return {@link IntegrationFlow}
     */
    @Bean
    public IntegrationFlow fileWriterFlow() {
        return IntegrationFlows
                .from(MessageChannels.direct("textInChannel"))              // input channel
                // .from("textInChannel")                                       // 隐式(自动)创建; 默认: DirectionChannel
                .<String, String>transform(String::toUpperCase)                 // transform
                .channel(MessageChannels.direct("fileWriterChannel"))       // output channel 隐式(自动)创建;默认: DirectionChannel
                .handle(Files.outboundAdapter(new File("~/files"))    // handle
                        .fileExistsMode(FileExistsMode.APPEND)
                        .appendNewLine(true))
                .get();
    }


    /**
     * Integration Flow.
     * <p>
     * Router:
     * <blockquote><pre>
     * even -> ...
     * odd ->  ...
     * </blockquote></pre>
     *
     * @return
     */
    @Bean("numberRoutingFlow")
    public IntegrationFlow numberRoutingFlow() {
        return IntegrationFlows
                .from("numberInChannel")
                .<Integer, String>route(n -> n % 2 == 0 ? "EVEN" : "ODD", mapping ->
                        mapping.subFlowMapping("EVEN", sf ->
                                        sf.<Integer, Integer>transform(n -> n * 10).handle((i, h) -> String.valueOf(i)))
                                .subFlowMapping("ODD", sf ->
                                        sf.<Integer, Integer>transform(n -> n + 1).handle((i, h) -> String.valueOf(i)))
                )
                .get();
    }

    /**
     * Integration Flow.
     * <p>
     * Router:
     * <blockquote><pre>
     *                      billing info    -> billing info channel
     * purchase order =>
     *                      line items      -> line items channel
     * </pre></blockquote>
     *
     * @return
     */
    @Bean
    public IntegrationFlow orderRoutingFlow() {
        return IntegrationFlows
                .from("purchaseOrderInChannel")
                .split(orderSplitter())
                .<Object, String>route(p -> {
                            if (p.getClass().isAssignableFrom(BillingInfo.class)) {
                                return "BILLING_INFO";
                            } else {
                                return "LINE_ITEMS";
                            }
                        }, mapping ->
                                mapping.subFlowMapping("BILLING_INFO", sf ->
                                                sf.<BillingInfo>handle((billingInfo, h) -> billingInfo))
                                        .subFlowMapping("LINE_ITEMS", sf ->
                                                sf.split().<LineItem>handle((lineItem, h) -> lineItem))
                )
                .get();
    }

    @Bean
    // @Splitter(inputChannel = "poChannel", outputChannel = "splitOrderChannel")
    public OrderSplitter orderSplitter() {
        return new OrderSplitter();
    }

    public class OrderSplitter {
        public Collection<Object> splitOrderIntoParts(final PurchaseOrder po) {
            ArrayList<Object> parts = new ArrayList<>();
            parts.add(po.getBillingInfo());
            parts.add(po.getLineItems());
            return parts;
        }
    }

    public static class PurchaseOrder {
        private BillingInfo billingInfo;
        private List<LineItem> lineItems;

        public BillingInfo getBillingInfo() {
            return billingInfo;
        }

        public void setBillingInfo(final BillingInfo billingInfo) {
            this.billingInfo = billingInfo;
        }

        public List<LineItem> getLineItems() {
            return lineItems;
        }

        public void setLineItems(final List<LineItem> lineItems) {
            this.lineItems = lineItems;
        }
    }

    public static class BillingInfo {
    }

    public static class LineItem {
    }

    /**
     * Channel.
     *
     * @return {@link MessageChannel}
     */
    @Bean("orderChannel")
    public MessageChannel orderChannel() {
        return new QueueChannel();
    }

    /**
     * Filter.
     *
     * @param clazz
     * @param <S>
     * @return
     */
    @Filter(inputChannel = "textInChannel", outputChannel = "fileWriterChannel")
    public <T> boolean payloadTypeFilter(final Message<T> message) {
        return new PayloadTypeSelector(String.class).accept(message);
    }


    /**
     * Transform.
     *
     * @return
     */
    @Bean("romanNumTransformer")
    @Transformer(inputChannel = "numberChannel")
    public GenericTransformer<Integer, String> numTransformer() {
        return String::valueOf;
    }

    /**
     * Router.
     * <p>
     * 偶数 -> evenChannel
     * 奇数 -> oddChannel
     *
     * @return
     */
    @Bean
    @Router(inputChannel = "numberChannel")
    public AbstractMessageRouter evenOddRouter() {
        return new AbstractMessageRouter() {
            @Override
            protected Collection<MessageChannel>
            determineTargetChannels(Message<?> message) {
                Integer number = (Integer) message.getPayload();

                return number % 2 == 0
                        ? Collections.singleton(evenChannel())
                        : Collections.singleton(oddChannel());
            }
        };
    }

    @Bean
    public MessageChannel evenChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel oddChannel() {
        return new DirectChannel();
    }
}