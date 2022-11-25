package com.example.springbatchintegrationsample;

import com.example.springbatchintegrationsample.batch.FirstJobDemo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
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
    private final JobRepository jobRepository;
    private final FirstJobDemo firstJobDemo;


    @Autowired
    public JobController(final FirstJobDemo firstJobDemo, final JobLauncher jobLauncher, final JobRepository jobRepository) {
        this.jobLauncher = jobLauncher;
        this.jobRepository = jobRepository;
        this.firstJobDemo = firstJobDemo;
    }

    @GetMapping("job")
    public void job() throws NoSuchAlgorithmException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        final Job job = firstJobDemo.firstJob();


        final JobParameter jobParameter = new JobParameter(SecureRandom.getInstanceStrong().nextLong(), true);
        final HashMap<String, JobParameter> jobParameterHashMap = new HashMap<>();
        jobParameterHashMap.put("job-id", jobParameter);
        final JobParameters jobParameters = new JobParameters(jobParameterHashMap);

        final JobExecution execution = jobLauncher.run(job, jobParameters);

        final Date startTime = execution.getStartTime();
        final Date endTime = execution.getEndTime();

        log.info("{}", startTime);
        log.info("{}", endTime);

    }
}