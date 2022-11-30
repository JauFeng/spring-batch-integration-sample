package com.example.springbatchintegrationsample.batch;

import com.example.springbatchintegrationsample.batch.listener.MyJobExecutionListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
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

    private final JobExecutionListener myJobExecutionListener;
    private final StepExecutionListener myStepExecutionListener;
    private final ChunkListener myChunkListener;


    @Autowired
    public SimpleJobDemo(final JobBuilderFactory jobBuilderFactory,
                         final StepBuilderFactory stepBuilderFactory,
                         final MyJobExecutionListener myJobExecutionListener,
                         final StepExecutionListener myStepExecutionListener,
                         final ChunkListener myChunkListener) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.myJobExecutionListener = myJobExecutionListener;
        this.myStepExecutionListener = myStepExecutionListener;
        this.myChunkListener = myChunkListener;
    }

    @Bean
    public Job simpleJob() {
        return jobBuilderFactory.get("simple-job")
                .start(simpleStep())
                .listener(myJobExecutionListener)
                .build();
    }

    private Step simpleStep() {
        return stepBuilderFactory.get("simple-step")
                .listener(myStepExecutionListener)
                .tasklet(simpleTasklet())
                .listener(myChunkListener)
                .build();
    }

    private Tasklet simpleTasklet() {
        return (contribution, chunkContext) ->
        {
            log.info("do something... ");
            return RepeatStatus.FINISHED;
        };
    }
}