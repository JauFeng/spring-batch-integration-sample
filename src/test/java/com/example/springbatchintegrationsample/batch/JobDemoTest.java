package com.example.springbatchintegrationsample.batch;

import com.example.springbatchintegrationsample.SpringBatchIntegrationSampleApplication;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {SpringBatchIntegrationSampleApplication.class})
class JobDemoTest {

    private final SimpleJobDemo firstJobDemo;

    @Autowired
    JobDemoTest(final SimpleJobDemo firstJobDemo) {
        this.firstJobDemo = firstJobDemo;
    }

    @Test
    void firstJob() {
        final Job job = firstJobDemo.simpleJob();


        System.out.println("Done.");
    }
}