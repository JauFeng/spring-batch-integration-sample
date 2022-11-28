package com.example.springbatchintegrationsample.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.DayOfWeek;
import java.time.LocalDate;

/**
 * 条件Job.
 */
@Component
public class DeciderJobDemo {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;


    private JobExecutionDecider myDecider() {
        return (jobExecution, stepExecution) -> {
            final DayOfWeek dayOfWeek = LocalDate.now().getDayOfWeek();

            if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) {
                return new FlowExecutionStatus("weekend");
            } else {
                return new FlowExecutionStatus("workingDay");
            }
        };
    }

    @Bean
    public Job deciderJob() {
        return jobBuilderFactory.get("deciderJob")
                .start(step1())
                .on(ExitStatus.COMPLETED.getExitCode()) // 条件: 执行结果
                .to(myDecider())    // 条件: JobDecider.
                .from(myDecider()).on("weekend").to(step2())
                .from(myDecider()).on("workingDay").to(step3())
                .from(step3()).on("*").to(step4())
                .end()
                .build();
    }

    private Step step1() {
        return stepBuilderFactory.get("step1")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("执行步骤一操作。。。");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private Step step2() {
        return stepBuilderFactory.get("step2")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("执行步骤二操作。。。");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private Step step3() {
        return stepBuilderFactory.get("step3")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("执行步骤三操作。。。");
                    return RepeatStatus.FINISHED;
                }).build();
    }


    private Step step4() {
        return stepBuilderFactory.get("step4")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("执行步骤四操作。。。");
                    return RepeatStatus.FINISHED;
                }).build();
    }
}