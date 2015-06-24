package org.graylog2.jersey.container.netty;

import jersey.repackaged.com.google.common.collect.Maps;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class ChunkedRequestAssembler {
    private static final Logger LOG = LoggerFactory.getLogger(ChunkedRequestAssembler.class);
    private final ConcurrentMap<Channel, List<HttpChunk>> chunkMap;
    private final ConcurrentMap<Channel, HttpRequest> initialRequests;

    public ChunkedRequestAssembler() {
        this.chunkMap = Maps.newConcurrentMap();
        this.initialRequests = Maps.newConcurrentMap();
    }

    public void setup(Channel channel, HttpRequest httpRequest) {
        chunkMap.putIfAbsent(channel, new ArrayList<HttpChunk>());
        initialRequests.put(channel, httpRequest);
    }

    public HttpRequest assemble(Channel channel) {
        List<HttpChunk> chunkList = chunkMap.remove(channel);
        HttpRequest request = initialRequests.remove(channel);

        ChannelBuffer dstBuffer = ChannelBuffers.dynamicBuffer();
        request.setContent(dstBuffer);

        try {
            for (HttpChunk chunk : chunkList) {
                dstBuffer.writeBytes(chunk.getContent());
            }
        } catch (Exception e) {
            LOG.warn("Error while assembling HTTP request chunks", e);
        }

        return request;
    }

    public void addChunk(Channel channel, HttpChunk nextChunk) {
        if (nextChunk == null) {
            LOG.error("Chunk for channel {} was null.", channel);
            return;
        }

        final List<HttpChunk> chunks = chunkMap.get(channel);
        if (chunks == null) {
            LOG.error("Chunks for channel {} couldn't be found, skipping chunk.", channel);
        } else {
            chunks.add(nextChunk);
        }
    }
}
