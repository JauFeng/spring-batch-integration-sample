package com.example.springbatchintegrationsample.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Job Execution Listener.
 */
@Slf4j
@Component
public class MyJobExecutionListener implements JobExecutionListener {
    @Override
    public void beforeJob(final JobExecution jobExecution) {
        log.debug(">>>> Before Job >>>>\n{}", jobExecution);
    }

    @Override
    public void afterJob(final JobExecution jobExecution) {
        log.debug("<<<< After Job <<<<\n{}", jobExecution);
    }
}