package com.example.springbatchintegrationsample.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyItemReadListener<T> implements ItemReadListener<T> {
    @Override
    public void beforeRead() {
        log.debug(">>>> Before item Read >>>>");
    }

    @Override
    public void afterRead(final T item) {
        log.debug("<<<< After item Read <<<<\n{}", item);
    }

    @Override
    public void onReadError(final Exception ex) {
        log.error("**** On item Read error ****", ex);
    }
}