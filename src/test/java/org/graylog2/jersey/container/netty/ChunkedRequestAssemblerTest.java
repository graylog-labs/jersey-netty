package org.graylog2.jersey.container.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ChunkedRequestAssemblerTest {
    private static final HttpChunk EMPTY_CHUNK = new HttpChunk() {
        @Override
        public boolean isLast() {
            return false;
        }

        @Override
        public ChannelBuffer getContent() {
            return ChannelBuffers.EMPTY_BUFFER;
        }

        @Override
        public void setContent(ChannelBuffer content) {
        }
    };
    private ChunkedRequestAssembler chunkedRequestAssembler;

    @BeforeMethod
    public void setUp() throws Exception {
        chunkedRequestAssembler = new ChunkedRequestAssembler();
    }

    @Test
    public void addChunkHandlesNonExistingChunkList() {
        DefaultLocalClientChannelFactory channelFactory = new DefaultLocalClientChannelFactory();
        DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
        Channel channel = channelFactory.newChannel(pipeline);
        chunkedRequestAssembler.addChunk(channel, EMPTY_CHUNK);
    }

    @Test
    public void addChunkHandlesNullChunk() {
        DefaultLocalClientChannelFactory channelFactory = new DefaultLocalClientChannelFactory();
        DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
        Channel channel = channelFactory.newChannel(pipeline);
        chunkedRequestAssembler.addChunk(channel, null);
    }

    @Test
    public void addChunkHandlesExistingChunkList() {
        DefaultLocalClientChannelFactory channelFactory = new DefaultLocalClientChannelFactory();
        DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
        Channel channel = channelFactory.newChannel(pipeline);
        HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "");
        chunkedRequestAssembler.setup(channel, httpRequest);
        chunkedRequestAssembler.addChunk(channel, EMPTY_CHUNK);
    }
}