package com.example.springbatchintegrationsample.batch.step;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * 多线程步骤.
 */
@Component
public class ThreadStepDemo {

    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public ThreadStepDemo(final StepBuilderFactory stepBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("spring_batch");
    }

    @Bean
    public Step sampleMultiStep(final TaskExecutor taskExecutor) {
        final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5);
        final ListItemReader<Integer> itemReader = new ListItemReader<>(source);
        final ListItemWriter<Object> itemWriter = new ListItemWriter<>();

        return this.stepBuilderFactory.get("sampleMultiStep")
                .<Integer, Long>chunk(2)
                .reader(itemReader)
                .writer(itemWriter)
                .taskExecutor(taskExecutor)
                .throttleLimit(5)
                .build();
    }
}