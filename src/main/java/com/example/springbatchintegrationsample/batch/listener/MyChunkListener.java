package com.example.springbatchintegrationsample.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyChunkListener implements ChunkListener {
    @Override
    public void beforeChunk(final ChunkContext context) {
        log.debug(">>>> Before Chunk. >>>>\n{}", context);
    }

    @Override
    public void afterChunk(final ChunkContext context) {
        log.debug("<<<< After Chunk <<<<\n{}", context);
    }

    @Override
    public void afterChunkError(final ChunkContext context) {
        log.error("**** After chunk error ****\n{}", context);
    }
}