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
        private final boolean connectionClose;
        private final Channel channel;
        private DefaultHttpResponse httpResponse;

        public NettyResponseWriter(HttpVersion protocolVersion, boolean connectionClose, Channel channel) {
            this.protocolVersion = protocolVersion;
            this.connectionClose = connectionClose;
            this.channel = channel;
        }

        @Override
        public OutputStream writeResponseStatusAndHeaders(long contentLength, ContainerResponse responseContext) throws ContainerException {
            log.trace("Writing response status and headers {}", responseContext);
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
            // TODO do we want to support this?
            log.debug("Trying to suspend for {} ms", timeUnit.toMillis(timeOut));
            return false;
        }

        @Override
        public void setSuspendTimeout(long timeOut, TimeUnit timeUnit) throws IllegalStateException {
            // TODO do we want to support this?
            log.debug("Setting suspend timeout to {} ms", timeUnit.toMillis(timeOut));
        }

        @Override
        public void commit() {
            final ChannelFuture channelFuture = channel.write(httpResponse);

            // keep the connection open if we are using HTTP 1.1
            if (protocolVersion == HttpVersion.HTTP_1_0 || connectionClose) {
                log.trace("Closing HTTP connection");
                channelFuture.addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void failure(Throwable error) {
            log.error("Uncaught exception in transport layer. This is likely a bug, closing channel.", error);
            if (channel.isOpen()) {
                channel.close();
            }
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

        // save the protocol version in case we encounter an exception, where we need it to construct the proper response
        final HttpVersion protocolVersion = httpRequest.getProtocolVersion();
        ctx.setAttachment(protocolVersion);

        containerRequest.setEntityStream(new ChannelBufferInputStream(httpRequest.getContent()));

        // for HTTP 1.0 we always close the connection after the request, for 1.1 we look at the Connection header
        boolean closeConnection = protocolVersion == HttpVersion.HTTP_1_0;
        final String connectionHeader = httpRequest.getHeader(HttpHeaders.Names.CONNECTION);
        if (connectionHeader != null && connectionHeader.equals("close")) {
            closeConnection = true;
        }
        containerRequest.setWriter(new NettyResponseWriter(protocolVersion,
                closeConnection, e.getChannel()));

        // *sigh*, netty has a list of Map.Entry and jersey wants a map. :/
        final MultivaluedMap<String,String> headers = containerRequest.getHeaders();
        for (Map.Entry<String, String> header : httpRequest.getHeaders()) {
            headers.add(header.getKey(), header.getValue());
        }
        appHandler.handle(containerRequest);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        log.error("Uncaught exception during jersey resource handling", e.getCause());
        final Channel channel = ctx.getChannel();
        if (! channel.isOpen()) {
            log.info("Not writing any response, channel is already closed.", e.getCause());
            return;
        }
        final HttpVersion protocolVersion = (HttpVersion) ctx.getAttachment();
        final boolean shouldCloseChannel = protocolVersion == HttpVersion.HTTP_1_0;

        final DefaultHttpResponse response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        new ChannelBufferOutputStream(buffer).writeBytes(e.toString());
        response.setContent(buffer);

        final ChannelFuture channelFuture = channel.write(response);

        if (shouldCloseChannel) {
            channelFuture.addListener(ChannelFutureListener.CLOSE);
        }
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
