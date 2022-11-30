package com.example.springbatchintegrationsample.both;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;

import java.io.File;

public class FileMessageToJobRequest {
    private Job job;
    private String fileParameterName;

    public void setFileParameterName(final String fileParameterName) {
        this.fileParameterName = fileParameterName;
    }

    public void setJob(final Job job) {
        this.job = job;
    }

    @Transformer
    public JobLaunchRequest toRequest(final Message<File> message) {
        final JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();

        jobParametersBuilder.addString(fileParameterName, message.getPayload().getAbsolutePath());

        return new JobLaunchRequest(job, jobParametersBuilder.toJobParameters());
    }
}