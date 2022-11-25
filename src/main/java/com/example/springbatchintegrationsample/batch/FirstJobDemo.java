package com.example.springbatchintegrationsample.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class FirstJobDemo {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public FirstJobDemo(final JobBuilderFactory jobBuilderFactory, final StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public Job firstJob() {
        return jobBuilderFactory.get("first-job")
                .start(firstStep())
                .build();
    }

    private Step firstStep() {
        return stepBuilderFactory.get("first-step")
                .tasklet((contribution, chunkContext) -> {
                    log.info("do something... ");
                    return RepeatStatus.FINISHED;
                }).build();
    }
}