package org.graylog2.jersey.container.netty;

import com.google.common.collect.Maps;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public class ChunkedRequestAssembler {
    private static final Logger log = LoggerFactory.getLogger(ChunkedRequestAssembler.class);
    private static final Map<Channel, List<Object>> chunkMap = Maps.newConcurrentMap();

    private final Channel channel;

    public ChunkedRequestAssembler(Channel channel) {
        this.channel = channel;
    }

    public ChunkedRequestAssembler(Channel channel, HttpRequest httpRequest) {
        this(channel);
        if (chunkMap.get(channel) == null)
            chunkMap.put(channel, new ArrayList<Object>());
        chunkMap.get(channel).add(httpRequest);
    }

    public HttpRequest assemble() {
        List<Object> chunkList = chunkMap.get(this.channel);

        HttpRequest request = (HttpRequest)chunkList.get(0);

        ChannelBuffer dstBuffer = ChannelBuffers.dynamicBuffer();
        request.setContent(dstBuffer);

        try {
            for (Object o : chunkList.subList(1, chunkList.size())) {
                HttpChunk chunk = (HttpChunk)o;
                dstBuffer.writeBytes(chunk.getContent());
            }
        } catch (Exception e) {
            log.warn("Caught exception: " + e);
        }

        return request;
    }

    public void addChunk(HttpChunk nextChunk) {
        chunkMap.get(this.channel).add(nextChunk);
    }
}
