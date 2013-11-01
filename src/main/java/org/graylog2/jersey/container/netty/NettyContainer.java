/*
 * Copyright 2013 Kay Roepke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graylog2.jersey.container.netty;

import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.internal.util.Base64;
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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NettyContainer extends SimpleChannelUpstreamHandler implements Container {
    private static final Logger log = LoggerFactory.getLogger(NettyContainer.class);

    public static final String PROPERTY_BASE_URI = "org.graylog2.jersey.container.netty.baseUri";

    private final ApplicationHandler appHandler;
    private SecurityContextFactory securityContextFactory;
    private final URI baseUri;

    public NettyContainer(ApplicationHandler appHandler) {
        this(appHandler, null);
    }

    public NettyContainer(ApplicationHandler appHandler, SecurityContextFactory securityContextFactory) {
        this.appHandler = appHandler;
        this.securityContextFactory = securityContextFactory;
        this.baseUri = (URI) this.getConfiguration().getProperty(PROPERTY_BASE_URI);
    }

    public void setSecurityContextFactory(SecurityContextFactory securityContextFactory) {
        this.securityContextFactory = securityContextFactory;
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
            httpResponse = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.valueOf(responseContext.getStatus()));

            long length = contentLength;
            if (length == -1 && responseContext.getEntity() instanceof String) { // TODO there's got to be a better way...
                final String entity = (String) responseContext.getEntity();
                final byte[] encodedBytes = entity.getBytes(Charset.forName("UTF-8"));
                length = encodedBytes.length;
            }
            if (! responseContext.getHeaders().containsKey(HttpHeaders.Names.CONTENT_LENGTH)) {
                HttpHeaders.setContentLength(httpResponse, length);
                log.trace("Writing response status and headers {}, length {}", responseContext, length);
            }

            for (Map.Entry<String, List<Object>> headerEntry : responseContext.getHeaders().entrySet()) {
                httpResponse.addHeader(headerEntry.getKey(), join(headerEntry.getValue(), ", "));
            }

            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            httpResponse.setContent(buffer);
            return new ChannelBufferOutputStream(buffer);
        }

        private static String join(List<Object> list, String delimiter) {
            final StringBuilder sb = new StringBuilder();
            String currentDelimiter = "";

            for(Object o : list) {
                sb.append(currentDelimiter);
                sb.append(o.toString());
                currentDelimiter = delimiter;
            }

            return sb.toString();
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

        // default to a simple security context factory, which is mostly useless, really.
        if (securityContextFactory == null) {
            securityContextFactory = new DefaultSecurityContextFactory();
        }
        // TODO we currently only support Basic Auth
        String[] schemeCreds = extractBasicAuthCredentials(httpRequest.getHeader(HttpHeaders.Names.AUTHORIZATION));
        String scheme = null;
        String user = null;
        String password = null;
        if (schemeCreds != null) {
            scheme = schemeCreds[0];
            user = schemeCreds[1];
            password = schemeCreds[2];
        }

        boolean isSecure = requestUri.getScheme().equalsIgnoreCase("https");
        ContainerRequest containerRequest = new ContainerRequest(
                baseUri,
                requestUri,
                httpRequest.getMethod().getName(),
                securityContextFactory.create(user, password, isSecure, scheme, e.getRemoteAddress().toString()),
                new MapPropertiesDelegate()
        );

        // save the protocol version in case we encounter an exception, where we need it to construct the proper response
        final HttpVersion protocolVersion = httpRequest.getProtocolVersion();
        ctx.setAttachment(httpRequest);

        containerRequest.setEntityStream(new ChannelBufferInputStream(httpRequest.getContent()));

        // copy the incoming headers over...
        final MultivaluedMap<String, String> incomingHeaders = containerRequest.getHeaders();
        for (Map.Entry<String, String> headerEntry : httpRequest.getHeaders()) {
            incomingHeaders.add(headerEntry.getKey(), headerEntry.getValue());
        }

        // for HTTP 1.0 we always close the connection after the request, for 1.1 we look at the Connection header
        boolean closeConnection = protocolVersion == HttpVersion.HTTP_1_0;
        final String connectionHeader = httpRequest.getHeader(HttpHeaders.Names.CONNECTION);
        if (connectionHeader != null && connectionHeader.equals("close")) {
            closeConnection = true;
        }
        containerRequest.setWriter(new NettyResponseWriter(protocolVersion,
                closeConnection, e.getChannel()));

        appHandler.handle(containerRequest);

        // *sigh*, netty has a list of Map.Entry and jersey wants a map. :/
        final MultivaluedMap<String, String> headers = containerRequest.getHeaders();
        for (Map.Entry<String, String> header : httpRequest.getHeaders()) {
            headers.add(header.getKey(), header.getValue());
        }

    }

    /* horrible, looks like rubby */
    private String[] extractBasicAuthCredentials(String authorizationHeader) {
        if (authorizationHeader == null) {
            return null;
        }
        String[] schemeUserPass = new String[3];
        String credentials;
        final String[] headerParts = authorizationHeader.split(" ");
        if (headerParts != null && headerParts.length == 2) {
            schemeUserPass[0] = headerParts[0].equalsIgnoreCase("basic") ? SecurityContext.BASIC_AUTH : null;
            credentials = Base64.decodeAsString(headerParts[1]);
            final String[] userPass = credentials.split(":");
            if (userPass != null && userPass.length == 2) {
                schemeUserPass[1] = userPass[0].replaceAll("%40", "@");
                schemeUserPass[2] = userPass[1];
            }
            return schemeUserPass;
        }
        return null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        log.error("Uncaught exception during jersey resource handling", e.getCause());
        final Channel channel = ctx.getChannel();
        if (!channel.isOpen()) {
            log.info("Not writing any response, channel is already closed.", e.getCause());
            return;
        }
        final HttpRequest request = (HttpRequest) ctx.getAttachment();
        final HttpVersion protocolVersion = request.getProtocolVersion();

        final DefaultHttpResponse response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        new ChannelBufferOutputStream(buffer).writeBytes(e.toString());
        response.setContent(buffer);

        final ChannelFuture channelFuture = channel.write(response);

        if ((protocolVersion == HttpVersion.HTTP_1_0)
                || request.getHeader(HttpHeaders.Names.CONNECTION).equalsIgnoreCase("close")) {
            channelFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

}
