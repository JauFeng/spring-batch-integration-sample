package com.example.springbatchintegrationsample.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MySkipListener<T, S> implements SkipListener<T, S> {
    @Override
    public void onSkipInRead(final Throwable t) {

    }

    @Override
    public void onSkipInWrite(final S item, final Throwable t) {

    }

    @Override
    public void onSkipInProcess(final T item, final Throwable t) {

    }
}