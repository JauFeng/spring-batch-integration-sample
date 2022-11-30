package com.example.springbatchintegrationsample.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyItemProcessListener<T, S> implements ItemProcessListener<T, S> {
    @Override
    public void beforeProcess(final T item) {
        log.debug(">>>> Before item Process >>>>\n{}", item);
    }

    @Override
    public void afterProcess(final T item, final S result) {
        log.debug("<<<< Before item Process <<<<\n item: {}, result: {}", item, result);
    }

    @Override
    public void onProcessError(final T item, final Exception e) {
        log.error("**** On process error ****\n{}", item, e);
    }
}