package com.example.springbatchintegrationsample.batch.listener;


import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyRetryListener implements RetryListener {
    @Override
    public <T, E extends Throwable> boolean open(final RetryContext context, final RetryCallback<T, E> callback) {
        return false;
    }

    @Override
    public <T, E extends Throwable> void close(final RetryContext context, final RetryCallback<T, E> callback, final Throwable throwable) {

    }

    @Override
    public <T, E extends Throwable> void onError(final RetryContext context, final RetryCallback<T, E> callback, final Throwable throwable) {

    }
}