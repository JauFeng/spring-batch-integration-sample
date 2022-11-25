# Spring Integration.

---

### 集成流:

声明集成流的三个配置选项包括:

* XML 配置
* Java 配置
* 使用 DSL 进行 Java 配置

---

### 组件:

集成流由以下一个或多个组件组成。

* Channels —— 将信息从一个元素传递到另一个元素。
* Filters —— 有条件地允许基于某些标准的消息通过流。
* Transformers —— 更改消息值或将消息有效负载从一种类型转换为另一种类型。
* Routers —— 直接将信息发送到几个渠道之一，通常是基于消息头。
* Splitters —— 将收到的信息分成两条或多条，每条都发送到不同的渠道。
* Aggregators —— 与分离器相反，它将来自不同渠道的多条信息组合成一条信息。
* Service activators —— 将消息传递给某个 Java 方法进行处理，然后在输出通道上发布返回值。
* Channel adapters —— 将通道连接到某些外部系统或传输。可以接受输入，也可以向外部系统写入。
* Gateways —— 通过接口将数据传递到集成流。

---

#### Channel(消息通道)

> 消息通道意指消息移动的集成管道移动。它们是连接 Spring Integration 所有其他部分的管道。

`Java配置` 和 `Java DSLChannel` 自动创建 `Channel` (默认: `DirectChannel`).

* `PublishSubscribeChannel` —— 消息被发布到 `PublishSubscribeChannel` 后又被传递给一个或多个消费者。如果有多个消费者，他们都将会收到消息。
* `QueueChannel` —— 消息被发布到 `QueueChannel` 后被存储到一个队列中，直到消息被消费者以先进先出（FIFO）的方式拉取。如果有多个消费者，他们中只有一个能收到消息。
    * 必须配置 `Poller`. `Poller(fixedRate="1000")` : 1000ms轮询一次.
* `PriorityChannel` —— 与 `QueueChannel` 类似，但是与 FIFO 方式不同，消息被冠以 priority 的消费者拉取。
* `RendezvousChannel` —— 与 `QueueChannel` 期望发送者阻塞通道，直到消费者接收这个消息类似，这种方式有效的同步了发送者与消费者。
* `DirectChannel` —— 与 `PublishSubscribeChannel` 类似，但是是通过在与发送方**相同的线程**中调用消费者来将消息发送给**
  单个消费者**，此通道类型**允许事务**跨越通道。
* `ExecutorChannel` —— 与 `DirectChannel` 类似，但是消息分派是通过 `TaskExecutor` 进行的，在与发送方**不同的线程**
  中进行，此通道类型**不支持事务**跨通道。
* `FluxMessageChannel` —— Reactive Streams Publisher 基于 Project Reactor Flux 的消息通道。

---

#### Filter(过滤器)

* Lambda表达式: `Integration.filter(lambda)`.
* `@Filer` 注解声明.
* `GenericSelector` 接口实现.

---

#### Transform(转换器)

* Lambda表达式: `Integration.transform(lambda)`.
* `@Bean` & `@Transform` 注解声明.
* `GenericTransformer` 接口实现.

---

#### Router(路由器)

> 基于某些路由标准的路由器允许在集成流中进行分支，将消息定向到不同的通道。

* 显式声明:

```java
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
                protected Collection<MessageChannel> determineTargetChannels(final Message<?> message) {
                        Integer number = (Integer) message.getPayload();
                        
                        return number % 2 == 0
                                ? Collections.singleton(evenChannel())  // even Channel
                                : Collections.singleton(oddChannel());  // odd Channel
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
```

* Java DSL

```java
   /**
     * Integration Flow.
     * <p>
     * Router:
     * even -> ...
     * odd ->  ...
     *
     * @return
     */
    @Bean
    public IntegrationFlow numberRoutingFlow() {
        return IntegrationFlows
                .from("")
                .<Integer, String>route(n -> n % 2 == 0 ? "EVEN" : "ODD", mapping ->
                        mapping.subFlowMapping("EVEN", sf ->
                                        sf.<Integer, Integer>transform(n -> n * 10).handle((i, h) -> String.valueOf(i)))
                                .subFlowMapping("ODD", sf ->
                                        sf.<Integer, Integer>transform(n -> n + 1).handle((i, h) -> String.valueOf(i)))
                )
                .get();
    }
```

---

#### Splitter(分割器)

> 将消息分割并处理.

##### 基本用例(设计思路)

**消息** Payload :
1.   `Message` 相同; 所有**不同类型信息**都包含在同一个`Message`中.
2.   分离`Message` -> `Collection<Object>`: 分割出消息负载中各种不同类型信息, 返回`Collection<Object>`.
```java
public class OrderSplitter {
    public Collection<Object> splitOrderIntoParts(PurchaseOrder po) {
        ArrayList<Object> parts = new ArrayList<>();
        parts.add(po.getBillingInfo());
        parts.add(po.getLineItems());
        return parts;
    }
}
```
3.   使用 `@splitter`, 汇聚流到一个 `channel`.
```java
@Bean
@Splitter(inputChannel="poChannel", outputChannel="splitOrderChannel")
public OrderSplitter orderSplitter() {
    return new OrderSplitter();
}
```
4.   使用 `Router` 路由到不同的 `channel` 处理不同类型的消息.
```java
@Bean
@Router(inputChannel="splitOrderChannel")
public MessageRouter splitOrderRouter() {
    PayloadTypeRouter router = new PayloadTypeRouter();
    router.setChannelMapping(BillingInfo.class.getName(), "billingInfoChannel");
    router.setChannelMapping(List.class.getName(), "lineItemsChannel");
    return router;
}
```

Java DSL:
```java
return IntegrationFlows
    ...
    .split(orderSplitter())
    .<Object, String> route(p -> {
        if (p.getClass().isAssignableFrom(BillingInfo.class)) {
            return "BILLING_INFO";
        } else {
            return "LINE_ITEMS";
        }
    }, mapping ->
           mapping.subFlowMapping("BILLING_INFO", sf -> 
                      sf.<BillingInfo> handle((billingInfo, h) -> { ... }))
                  .subFlowMapping("LINE_ITEMS", sf -> 
                       sf.split().<LineItem> handle((lineItem, h) -> { ... }))
    )
    .get();
```

**信息** Payload :

因为 `Input Channel` 不同, 直接转到不同 `Output Channel` 处理.

---

#### ServiceActivator(服务激活器)

>接收 `Input Channel` -> 返回 `MessageHandler`

显式声明:

```java
@Bean
@ServiceActivator(inputChannel="someChannel")
public MessageHandler sysoutHandler() {
    return message -> {
        System.out.println("Message payload: " + message.getPayload());
    };
}

@Bean
@ServiceActivator(inputChannel="orderChannel", outputChannel="completeOrder")
public GenericHandler<Order> orderHandler(OrderRepository orderRepo) {
    return (payload, headers) -> {
        return orderRepo.save(payload);
    };
}
```

Java DSL:
```java
public IntegrationFlow someFlow() {
    return IntegrationFlows
        ...
        .handle(msg -> {
            System.out.println("Message payload: " + msg.getPayload());
        })
        .get();
}
```
```java
public IntegrationFlow orderFlow(OrderRepository orderRepo) {
    return IntegrationFlows
        ...
        .<Order>handle((payload, headers) -> {
            return orderRepo.save(payload);
        })
        ...
        .get();
}
```

---

#### Gateway(网关)

>网关: 将数据提交到一个集成信息流和接收该流响应的装置.

显式声明:
```java
@Component
@MessagingGateway(defaultRequestChannel="inChannel", defaultReplyChannel="outChannel")
public interface UpperCaseGateway {
    String uppercase(String in);
}
```

Java DSL:
```java
@Bean
public IntegrationFlow uppercaseFlow() {
    return IntegrationFlows
        .from("inChannel")
        .<String, String> transform(s -> s.toUpperCase())
        .channel("outChannel")
        .get();
}
```
---

#### InboundChannelAdapter(通道适配器)

> 入口 & 出口. <br>
> 由spring-integration-xxx module 实现. <br>
> 入站: `MessageSource` <br>
> 出站: `MessageHandler` & `ServiceActivator`<br>

显式声明:
```java
@Bean
@InboundChannelAdapter(
    poller=@Poller(fixedRate="1000"), channel="numberChannel")
public MessageSource<Integer> numberSource(AtomicInteger source) {
    return () -> {
        return new GenericMessage<>(source.getAndIncrement());
    };
}
```

Java DSL:
```java
@Bean
public IntegrationFlow someFlow(AtomicInteger integerSource) {
    return IntegrationFlows
        .from(integerSource, "getAndIncrement",
              c -> c.poller(Pollers.fixedRate(1000)))
        ...
        .get();
}
```

##### 入站适配器: `MessageSource`

```java
@Bean
@InboundChannelAdapter(channel="file-channel", poller=@Poller(fixedDelay="1000"))
public MessageSource<File> fileReadingMessageSource() {
    FileReadingMessageSource sourceReader = new FileReadingMessageSource();
    sourceReader.setDirectory(new File(INPUT_DIR));
    sourceReader.setFilter(new SimplePatternFileListFilter(FILE_PATTERN));
    return sourceReader;
}
```
```java
@Bean
public IntegrationFlow fileReaderFlow() {
    return IntegrationFlows
        .from(Files.inboundAdapter(new File(INPUT_DIR))
              .patternFilter(FILE_PATTERN))
        .get();
}
```

##### 出站适配器: `MessageHandler` & `ServiceActivator`

```java
    @Bean
    public IntegrationFlow fileWriterFlow() {
        return IntegrationFlows
            .from(MessageChannels.direct("textInChannel"))
            .<String, String>transform(t -> t.toUpperCase())
            .handle(Files.outboundAdapter(new File("/tmp/sia5/files"))
                    .fileExistsMode(FileExistsMode.APPEND)
                    .appendNewLine(true))
            .get();
    }
```