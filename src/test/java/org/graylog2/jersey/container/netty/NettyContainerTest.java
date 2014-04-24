/*
 * Copyright 2014 TORCH GmbH
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.jersey.container.netty;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ning.http.client.*;
import org.glassfish.jersey.process.Inflector;
import org.glassfish.jersey.server.ChunkedOutput;
import org.glassfish.jersey.server.ContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.testng.annotations.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class NettyContainerTest {

    @Test
    public void testChunkedOutput() throws IOException, URISyntaxException, ExecutionException, InterruptedException {

        Inflector<ContainerRequestContext, ChunkedOutput<?>> inflector = new Inflector<ContainerRequestContext, ChunkedOutput<?>>() {

            @Override
            public ChunkedOutput<String> apply(ContainerRequestContext containerRequestContext) {
                final ChunkedOutput<String> output = new ChunkedOutput<String>(String.class);
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            String a = Strings.repeat("a", 8192);
                            String b = Strings.repeat("b", 8192);
                            output.write(a);
                            output.write(b);
                        } catch (IOException e) {
                            fail("writing failed", e);
                        }
                        try {
                            output.close();
                        } catch (IOException e) {
                            fail("closing", e);
                        }
                    }
                }.start();
                return output;
            }
        };
        final ServerBootstrap bootstrap = getServerBootstrap();
        int port = bindJerseyServer(inflector, bootstrap);

        final AsyncHttpClient client = getHttpClient();

        ListenableFuture<Object> response = client.prepareGet("http://localhost:" + port + "/")
                .execute(new AsyncHandler<Object>() {

                    private ArrayList<String> chunks = Lists.newArrayList(
                            Strings.repeat("a", 8192),
                            Strings.repeat("b", 8192)
                    );
                    private int chunkIdx = 0;

                    @Override
                    public void onThrowable(Throwable t) {
                        fail("Should not throw up", t);
                    }

                    @Override
                    public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                        String expected = chunks.get(chunkIdx);
                        String actual = new String(bodyPart.getBodyPartBytes());

                        assertEquals(actual.length(), expected.length());
                        assertEquals(actual, expected);
                        chunkIdx++;
                        return STATE.CONTINUE;
                    }

                    @Override
                    public STATE onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
                        assertEquals(responseStatus.getStatusCode(), 200);
                        return STATE.CONTINUE;
                    }

                    @Override
                    public STATE onHeadersReceived(HttpResponseHeaders headers) throws Exception {
                        return STATE.CONTINUE;
                    }

                    @Override
                    public Object onCompleted() throws Exception {
                        // we don't care, all asserts happen in the other callbacks
                        return "";
                    }
                });
        response.get();
        bootstrap.shutdown();
    }

    public static class Entity {
        public String line;
        public String header;
        private int chunksize = 0;

        public Entity(boolean first, int chunksize) {
            this.chunksize = chunksize;
            if (first) {
                header = "foo,bar,baz";
            } else {
                line = "foovalue{0},barvalue{0},bazvalue{0}";
            }
        }

        public byte[] getLineBytes() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < chunksize; i++) {
                String s = MessageFormat.format(line, i);
                sb.append(s).append("\n");
            }
            return sb.toString().getBytes();
        }
    }

    public static class EntityWriter implements MessageBodyWriter<Entity> {

        @Override
        public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
            return type.isAssignableFrom(Entity.class) && mediaType.equals(MediaType.TEXT_PLAIN_TYPE);
        }

        @Override
        public long getSize(Entity entity,
                            Class<?> type,
                            Type genericType,
                            Annotation[] annotations,
                            MediaType mediaType) {
            return -1;
        }

        @Override
        public void writeTo(Entity entity,
                            Class<?> type,
                            Type genericType,
                            Annotation[] annotations,
                            MediaType mediaType,
                            MultivaluedMap<String, Object> httpHeaders,
                            OutputStream entityStream) throws IOException, WebApplicationException {
            if (entity.header != null) {
                entityStream.write(entity.header.getBytes());
            } else {
                entityStream.write(entity.getLineBytes());
            }
        }
    }

    @Test
    public void testEntityChunkedOutput() throws URISyntaxException, IOException, ExecutionException, InterruptedException {

        Inflector<ContainerRequestContext, ChunkedOutput<?>> inflector = new Inflector<ContainerRequestContext, ChunkedOutput<?>>() {

            @Override
            public ChunkedOutput<Entity> apply(ContainerRequestContext containerRequestContext) {
                final ChunkedOutput<Entity> output = new ChunkedOutput<Entity>(Entity.class);
                new Thread() {
                    int i = 0;
                    @Override
                    public void run() {
                        try {
                            while (i <= 4) {
                                if (i == 0) {
                                    output.write(new Entity(true, 0));
                                } else {
                                    output.write(new Entity(false, 1000));
                                }
                                i++;
                            }
                            output.close();
                        } catch (IOException e) {
                            fail("writing should not fail", e);
                        }
                    }
                }.start();
                return output;
            }
        };
        ServerBootstrap bootstrap = getServerBootstrap();
        int port = bindJerseyServer(inflector, bootstrap, EntityWriter.class);

        final AsyncHttpClient client = getHttpClient();
        ListenableFuture<Object> request = client.prepareGet("http://localhost:" + port + "/").execute(new AsyncHandler<Object>() {
            @Override
            public void onThrowable(Throwable t) {
                fail("Should not throw up", t);
            }

            @Override
            public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                return STATE.CONTINUE;
            }

            @Override
            public STATE onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
                return STATE.CONTINUE;
            }

            @Override
            public STATE onHeadersReceived(HttpResponseHeaders headers) throws Exception {
                return STATE.CONTINUE;
            }

            @Override
            public Object onCompleted() throws Exception {
                return STATE.CONTINUE;
            }
        });
        request.get();
        bootstrap.shutdown();
    }

    private ServerBootstrap getServerBootstrap() {
        final ExecutorService bossExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                        .setNameFormat("restapi-boss-%d")
                        .build()
        );

        final ExecutorService workerExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                        .setNameFormat("restapi-worker-%d")
                        .build()
        );

        return new ServerBootstrap(new NioServerSocketChannelFactory(
                bossExecutor,
                workerExecutor
        ));
    }

    private void setChunkedHttpPipeline(ServerBootstrap bootstrap, final NettyContainer jerseyHandler) {
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new HttpRequestDecoder());
                pipeline.addLast("encoder", new HttpResponseEncoder());
                pipeline.addLast("chunks", new ChunkedWriteHandler());
                pipeline.addLast("jerseyHandler", jerseyHandler);
                return pipeline;
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
    }

    private Resource getResource(Inflector<ContainerRequestContext, ChunkedOutput<?>> inflector) {
        final Resource.Builder resourceBuilder = Resource.builder();
        resourceBuilder.path("/");
        final ResourceMethod.Builder methodBuilder = resourceBuilder.addMethod("GET");
        methodBuilder.produces(MediaType.TEXT_PLAIN_TYPE)
                .handledBy(inflector);
        return resourceBuilder.build();
    }

    private NettyContainer getNettyContainer(Resource resource, Class... classes) throws URISyntaxException {
        ResourceConfig rc = new ResourceConfig()
                .property(NettyContainer.PROPERTY_BASE_URI, new URI("http:/localhost:0"))
                .registerResources(resource)
                .registerInstances(new NettyContainerProvider())
                .register(JacksonJsonProvider.class);
        for (Class aClass : classes) {
            rc.register(aClass);
        }

        return ContainerFactory.createContainer(NettyContainer.class, rc);
    }

    private AsyncHttpClient getHttpClient() {
        final AsyncHttpClientConfig.Builder clientBuilder = new AsyncHttpClientConfig.Builder();
        clientBuilder.setAllowPoolingConnection(false);
        final AsyncHttpClientConfig config = clientBuilder.build();
        return new AsyncHttpClient(config);
    }

    private int bindJerseyServer(Inflector<ContainerRequestContext, ChunkedOutput<?>> inflector,
                                 ServerBootstrap bootstrap, Class... classes) throws URISyntaxException {
        final Resource resource = getResource(inflector);
        final NettyContainer jerseyHandler = getNettyContainer(resource, classes);
        setChunkedHttpPipeline(bootstrap, jerseyHandler);

        final Channel bind = bootstrap.bind(new InetSocketAddress(0));

        InetSocketAddress socketAddress = (InetSocketAddress) bind.getLocalAddress();
        return socketAddress.getPort();
    }
}
