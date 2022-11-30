package com.example.springbatchintegrationsample.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class MyItemWriteListener<S> implements ItemWriteListener<S> {
    @Override
    public void beforeWrite(final List<? extends S> items) {
        log.debug(">>>> Before item Write >>>>\n{}", items);
    }

    @Override
    public void afterWrite(final List<? extends S> items) {
        log.debug("<<<< After item Write <<<<\n{}", items);
    }

    @Override
    public void onWriteError(final Exception exception, final List<? extends S> items) {
        log.error("**** After item Writer error ****\n{}", items, exception);
    }
}