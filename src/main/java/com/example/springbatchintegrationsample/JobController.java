package com.example.springbatchintegrationsample;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

@Slf4j
@RestController
public class JobController {

    private final JobLauncher jobLauncher;

    private final Job simpleJob;

    private final Job remoteChunkingJob;

    public JobController(final JobLauncher jobLauncher,
                         final Job simpleJob,
                         final Job remoteChunkingJob) {
        this.jobLauncher = jobLauncher;
        this.simpleJob = simpleJob;
        this.remoteChunkingJob = remoteChunkingJob;
    }

    /**
     * Simple Job.
     */
    @GetMapping("simple-job")
    public String simpleJob() throws NoSuchAlgorithmException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        // 组装: JobParameters
        final JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("job-id", String.valueOf(SecureRandom.getInstanceStrong().nextLong()), true);
        final JobParameters jobParameters = jobParametersBuilder.toJobParameters();

        // 启动: Job
        final JobExecution execution = jobLauncher.run(simpleJob, jobParameters);

        return "success";
    }

    /**
     * Remote chunking job.
     */
    @GetMapping("remote-chunking-job")
    public String remoteChunkingJob() throws NoSuchAlgorithmException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        // 组装: JobParameters
        final JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("job-id", String.valueOf(SecureRandom.getInstanceStrong().nextLong()), true);
        final JobParameters jobParameters = jobParametersBuilder.toJobParameters();

        final JobExecution execution = jobLauncher.run(remoteChunkingJob, jobParameters);

        return "success";
    }
}