package com.example.springbatchintegrationsample.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


/**
 * 简单: Job.
 */
@Slf4j
@Component
public class SimpleJobDemo {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public SimpleJobDemo(final JobBuilderFactory jobBuilderFactory, final StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job simpleJob() {
        return jobBuilderFactory.get("simple-job")
                .start(simpleStep())
                .build();
    }

    private Step simpleStep() {
        return stepBuilderFactory.get("simple-step")
                .tasklet((contribution, chunkContext) -> {
                    log.info("do something... ");
                    return RepeatStatus.FINISHED;
                }).build();
    }
}