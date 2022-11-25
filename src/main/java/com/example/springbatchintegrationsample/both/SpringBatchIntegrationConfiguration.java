package com.example.springbatchintegrationsample.both;

import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.handler.LoggingHandler;

import java.io.File;

@Configuration
public class SpringBatchIntegrationConfiguration {

    /*
    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("input.file.name");
        fileMessageToJobRequest.setJob(personJob());
        return fileMessageToJobRequest;
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(new SyncTaskExecutor());
        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(simpleJobLauncher);

        return jobLaunchingGateway;
    }
    @Bean
    public IntegrationFlow integrationFlow(JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlows.from(
                        Files.inboundAdapter(new File("/tmp/myfiles")).filter(new SimplePatternFileListFilter("*.csv")),
                        c -> c.poller(Pollers.fixedRate(1000).maxMessagesPerPoll(1))).
                transform(fileMessageToJobRequest()).
                handle(jobLaunchingGateway).
                log(LoggingHandler.Level.WARN, "headers.id + ': ' + payload").
                get();
    }

*/

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