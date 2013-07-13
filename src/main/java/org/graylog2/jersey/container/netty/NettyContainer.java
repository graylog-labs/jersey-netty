package org.graylog2.jersey.container.netty;

import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.*;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerResponseWriter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import java.io.OutputStream;
import java.net.URI;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NettyContainer extends SimpleChannelUpstreamHandler implements Container {
    private static final Logger log = LoggerFactory.getLogger(NettyContainer.class);

    public static final String PROPERTY_BASE_URI = "org.graylog2.jersey.container.netty.baseUri";

    private final ApplicationHandler appHandler;
    private final URI baseUri;

    public NettyContainer(ApplicationHandler appHandler) {
        this.appHandler = appHandler;
        this.baseUri = (URI) this.getConfiguration().getProperty(PROPERTY_BASE_URI);
    }

    private static final class NettyResponseWriter implements ContainerResponseWriter {

        private final HttpVersion protocolVersion;
        private final Channel channel;
        private DefaultHttpResponse httpResponse;

        public NettyResponseWriter(HttpVersion protocolVersion, Channel channel) {
            this.protocolVersion = protocolVersion;
            this.channel = channel;
        }

        @Override
        public OutputStream writeResponseStatusAndHeaders(long contentLength, ContainerResponse responseContext) throws ContainerException {
            httpResponse = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.valueOf(responseContext.getStatus()));

            if (contentLength != -1) {
                HttpHeaders.setContentLength(httpResponse, contentLength);
            }
            for (Map.Entry<String, List<Object>> headerEntry : responseContext.getHeaders().entrySet()) {
                httpResponse.addHeader(headerEntry.getKey(), headerEntry.getValue());
            }

            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            httpResponse.setContent(buffer);
            return new ChannelBufferOutputStream(buffer);
        }

        @Override
        public boolean suspend(long timeOut, TimeUnit timeUnit, TimeoutHandler timeoutHandler) {
            return false;
        }

        @Override
        public void setSuspendTimeout(long timeOut, TimeUnit timeUnit) throws IllegalStateException {
        }

        @Override
        public void commit() {
            channel.write(httpResponse).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void failure(Throwable error) {
            channel.close();
        }

        @Override
        public boolean enableResponseBuffering() {
            return false;
        }
    }

    @Override
    public ResourceConfig getConfiguration() {
        return appHandler.getConfiguration();
    }

    @Override
    public void reload() {
        log.info("container reload");
        // TODO when is this called
    }

    @Override
    public void reload(ResourceConfig configuration) {
        log.info("container reload with new configuration {}", configuration);
        // TODO when is this called
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        HttpRequest httpRequest = (HttpRequest) e.getMessage();
        URI requestUri = baseUri.resolve(httpRequest.getUri());

        boolean isSecure = requestUri.getScheme().equalsIgnoreCase("https");
        ContainerRequest containerRequest = new ContainerRequest(
                baseUri,
                requestUri,
                httpRequest.getMethod().getName(),
                getSecurityContext(isSecure),
                new MapPropertiesDelegate()
        );

        containerRequest.setEntityStream(new ChannelBufferInputStream(httpRequest.getContent()));
        containerRequest.setWriter(new NettyResponseWriter(httpRequest.getProtocolVersion(), e.getChannel()));

        // *sigh*, netty has a list of Map.Entry and jersey wants a map. :/
        final MultivaluedMap<String,String> headers = containerRequest.getHeaders();
        for (Map.Entry<String, String> header : httpRequest.getHeaders()) {
            headers.add(header.getKey(), header.getValue());
        }
        appHandler.handle(containerRequest);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        log.error("Exception during jersey resource handling", e.getCause());
        super.exceptionCaught(ctx, e);
    }

    // TODO implement something useful here.
    private SecurityContext getSecurityContext(final boolean isSecure) {
        return new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return null;
            }

            @Override
            public boolean isUserInRole(String role) {
                return false;
            }

            @Override
            public boolean isSecure() {
                return isSecure;
            }

            @Override
            public String getAuthenticationScheme() {
                return null;
            }
        };
    }
}
