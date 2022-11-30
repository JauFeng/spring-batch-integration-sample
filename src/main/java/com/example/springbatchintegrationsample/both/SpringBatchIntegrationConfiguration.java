package com.example.springbatchintegrationsample.both;

import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.handler.LoggingHandler;

import java.nio.file.Paths;

@Configuration
public class SpringBatchIntegrationConfiguration {

    private final JobRepository jobRepository;

    public SpringBatchIntegrationConfiguration(final JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        final FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("input.file.name");
        // fileMessageToJobRequest.setJob(personJob());
        return fileMessageToJobRequest;
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        final SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(new SyncTaskExecutor());

        return new JobLaunchingGateway(simpleJobLauncher);
    }

    @Bean
    public IntegrationFlow integrationFlow(final JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlows.from(
                        Files.inboundAdapter(Paths.get("files").toFile()).filter(new SimplePatternFileListFilter("*.csv")),
                        c -> c.poller(Pollers
                                .fixedRate(1000)
                                .maxMessagesPerPoll(1))).
                transform(fileMessageToJobRequest()).
                handle(jobLaunchingGateway).
                log(LoggingHandler.Level.WARN, "headers.id + ': ' + payload").
                get();
    }

    @Bean
    @StepScope
    public ItemReader<String> sampleReader(@Value("#{jobParameters[input.file.name]}") String resource) {
        final FlatFileItemReader<String> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource(resource));
        return flatFileItemReader;
    }


    /**
     * Providing Feedback with Informational Messages.
     */
    @Bean
    @ServiceActivator(inputChannel = "stepExecutionsChannel")
    public LoggingHandler loggingHandler() {
        LoggingHandler adapter = new LoggingHandler(LoggingHandler.Level.WARN);
        adapter.setLoggerName("TEST_LOGGER");
        adapter.setLogExpressionString("headers.id + ': ' + payload");
        return adapter;
    }

    @MessagingGateway(name = "notificationExecutionsListener", defaultRequestChannel = "stepExecutionsChannel")
    public interface NotificationExecutionListener extends StepExecutionListener {
    }
}