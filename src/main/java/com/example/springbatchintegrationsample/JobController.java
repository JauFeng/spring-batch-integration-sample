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
import java.util.Date;
import java.util.HashMap;

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
        final JobParameter jobParameter = new JobParameter(SecureRandom.getInstanceStrong().nextLong(), true);
        final HashMap<String, JobParameter> jobParameterHashMap = new HashMap<>();
        jobParameterHashMap.put("job-id", jobParameter);
        final JobParameters jobParameters = new JobParameters(jobParameterHashMap);

        // 启动: Job
        final JobExecution execution = jobLauncher.run(simpleJob, jobParameters);

        // 执行时间
        final Date startTime = execution.getStartTime();
        final Date endTime = execution.getEndTime();

        log.info("startTime: {}", startTime);
        log.info("endTime: {}", endTime);

        return "success";
    }

    /**
     * Remote chunking job.
     */
    @GetMapping("remote-chunking-job")
    public String remoteChunkingJob() throws NoSuchAlgorithmException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        // 组装: JobParameters
        final JobParameter jobParameter = new JobParameter(SecureRandom.getInstanceStrong().nextLong(), true);
        final HashMap<String, JobParameter> jobParameterHashMap = new HashMap<>();
        jobParameterHashMap.put("job-id", jobParameter);
        final JobParameters jobParameters = new JobParameters(jobParameterHashMap);

        final JobExecution execution = jobLauncher.run(remoteChunkingJob, jobParameters);

        // 执行时间
        final Date startTime = execution.getStartTime();
        final Date endTime = execution.getEndTime();

        log.info("startTime: {}", startTime);
        log.info("endTime: {}", endTime);

        return "success";
    }
}