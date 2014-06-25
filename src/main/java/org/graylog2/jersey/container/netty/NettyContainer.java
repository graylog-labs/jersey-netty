/*
 * Copyright 2013-2014 TORCH GmbH
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
import org.glassfish.jersey.message.internal.HttpDateFormat;
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
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*
 OMG this is getting to be such a hack.
 */
public class NettyContainer extends SimpleChannelUpstreamHandler implements Container {
    private static final Logger log = LoggerFactory.getLogger(NettyContainer.class);

    public static final String PROPERTY_BASE_URI = "org.graylog2.jersey.container.netty.baseUri";
    public static final String REQUEST_PROPERTY_REMOTE_ADDR = "org.graylog2.jersey.container.netty.request.property.remote_addr";

    private final ApplicationHandler appHandler;
    private SecurityContextFactory securityContextFactory;
    private final URI baseUri;

    private final ChunkedRequestAssembler chunkedRequestAssembler;

    public NettyContainer(ApplicationHandler appHandler) {
        this(appHandler, null);
    }

    public NettyContainer(ApplicationHandler appHandler, SecurityContextFactory securityContextFactory) {
        this.appHandler = appHandler;
        this.securityContextFactory = securityContextFactory;
        this.baseUri = (URI) this.getConfiguration().getProperty(PROPERTY_BASE_URI);
        this.chunkedRequestAssembler = new ChunkedRequestAssembler();
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
                HttpHeaders.addHeader(httpResponse, headerEntry.getKey(), join(headerEntry.getValue(), ", "));
            }
            if (protocolVersion.equals(HttpVersion.HTTP_1_1) && HttpHeaders.getContentLength(httpResponse, -3L) != -3L) {
                httpResponse.setChunked(true);
                HttpHeaders.setTransferEncodingChunked(httpResponse);
                // write the first chunk's headers right away
                channel.write(httpResponse);

                // be sure to copy the arrays into buffers here, because they get re-used internally!
                return new OutputStream() {
                    @Override
                    public void write(byte[] b, int off, int len) {
                        final ChannelBuffer buffer = ChannelBuffers.copiedBuffer(b, off, len);
                        if (log.isTraceEnabled()) {
                            log.trace("writing data: {}", buffer.toString(Charset.defaultCharset()));
                        }
                        channel.write(new DefaultHttpChunk(buffer));
                        if (log.isDebugEnabled()) {
                            log.debug("wrote {} bytes as chunk", len);
                        }
                    }

                    @Override
                    public void write(int b) {
                        ChannelBuffer content = ChannelBuffers.copiedBuffer(new byte[]{(byte) b});
                        if (log.isTraceEnabled()) {
                            log.trace("writing data: {}", content.toString(Charset.defaultCharset()));
                        }
                        channel.write(new DefaultHttpChunk(content));
                    }
                };
            } else {
                // we also need to write the response into the same http message if we don't chunk the response.
                httpResponse.setContent(ChannelBuffers.dynamicBuffer());
                return new ChannelBufferOutputStream(httpResponse.getContent());
            }
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
            log.debug("Trying to suspend for {} ms, handler {}", timeUnit.toMillis(timeOut), timeoutHandler);
            return false;
        }

        @Override
        public void setSuspendTimeout(long timeOut, TimeUnit timeUnit) throws IllegalStateException {
            // TODO do we want to support this?
            log.debug("Setting suspend timeout to {} ms", timeUnit.toMillis(timeOut));
        }

        @Override
        public void commit() {
            if (channel.isOpen()) {
                final ChannelFuture channelFuture;
                if (httpResponse.isChunked()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Writing last chunk to {}", channel.getRemoteAddress());
                    }
                    channelFuture = channel.write(new DefaultHttpChunkTrailer());
                } else {
                    // we don't chunk the response so we simply write it in one go.
                    if (log.isTraceEnabled()) {
                        log.trace("Writing entire {} bytes to client {}",
                                  httpResponse.getContent().readableBytes(),
                                  channel.getRemoteAddress());
                    }
                    channelFuture = channel.write(httpResponse);
                }
                if (connectionClose) {
                    log.debug("Closing connection to {}", channel.getRemoteAddress());
                    channelFuture.addListener(ChannelFutureListener.CLOSE);
                } else {
                    channelFuture.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                }
            }
        }

        @Override
        public void failure(Throwable error) {
            log.error("Uncaught exception in transport layer. This is likely a bug, closing channel.", error);
            if (channel.isOpen()) {
                if (channel.isWritable()) {
                    final DefaultHttpResponse internalServerResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    try {
                        internalServerResponse.setContent(ChannelBuffers.wrappedBuffer(("Uncaught exception!\n"
                                + error.getMessage()).getBytes("UTF-8")));
                    } catch (UnsupportedEncodingException ignored) {}
                    channel.write(internalServerResponse).addListener(ChannelFutureListener.CLOSE);
                } else {
                    channel.close();
                }
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
        HttpRequest httpRequest = null;
        if (e.getMessage() instanceof DefaultHttpRequest) {
            httpRequest = (DefaultHttpRequest) e.getMessage();
            if (httpRequest.isChunked()) {
                chunkedRequestAssembler.setup(e.getChannel(), httpRequest);

                String expectHeader = HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.EXPECT);
                if (expectHeader != null && expectHeader.equals("100-continue")) {
                    final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE);
                    final ChannelFuture channelFuture = e.getChannel().write(response);
                }
                return;
            }
        }
        else if (e.getMessage() instanceof HttpChunk) {
            HttpChunk nextChunk = (HttpChunk)e.getMessage();
            chunkedRequestAssembler.addChunk(e.getChannel(), nextChunk);

            if (nextChunk.isLast()) {
                httpRequest = chunkedRequestAssembler.assemble(e.getChannel());
            } else {
                final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE);
                final ChannelFuture channelFuture = e.getChannel().write(response);
                return;
            }
        }

        URI requestUri;
        try {
            requestUri = baseUri.resolve(httpRequest.getUri());
        } catch (IllegalArgumentException throwable) {
            log.debug("Client sent invalid URL. Closing connection.");
            ExceptionEvent exceptionEvent = new DefaultExceptionEvent(ctx.getChannel(), throwable);
            invalidRequestSent(ctx, exceptionEvent);
            return;
        }

        // default to a simple security context factory, which is mostly useless, really.
        if (securityContextFactory == null) {
            securityContextFactory = new DefaultSecurityContextFactory();
        }
        // TODO we currently only support Basic Auth
        String[] schemeCreds = extractBasicAuthCredentials(HttpHeaders.getHeader(httpRequest,
                                                                                 HttpHeaders.Names.AUTHORIZATION));
        String scheme = null;
        String user = null;
        String password = null;
        if (schemeCreds != null) {
            scheme = schemeCreds[0];
            user = schemeCreds[1];
            password = schemeCreds[2];
        }

        boolean isSecure = requestUri.getScheme().equalsIgnoreCase("https");
        final SecurityContext securityContext = securityContextFactory.create(user,
                                                                              password,
                                                                              isSecure,
                                                                              scheme,
                                                                              ctx.getChannel().getRemoteAddress().toString());
        ContainerRequest containerRequest = new ContainerRequest(
                baseUri,
                requestUri,
                httpRequest.getMethod().getName(),
                securityContext,
                new MapPropertiesDelegate()
        );
        final SocketAddress remoteAddress = ctx.getChannel().getRemoteAddress();
        containerRequest.setProperty(REQUEST_PROPERTY_REMOTE_ADDR, remoteAddress);

        // save the protocol version in case we encounter an exception, where we need it to construct the proper response
        final HttpVersion protocolVersion = httpRequest.getProtocolVersion();
        ctx.setAttachment(httpRequest);

        containerRequest.setEntityStream(new ChannelBufferInputStream(httpRequest.getContent()));

        // copy the incoming headers over...
        final MultivaluedMap<String, String> incomingHeaders = containerRequest.getHeaders();
        // this is the case for ShiroSecurityContext
        if (securityContext instanceof HeaderAwareSecurityContext) {
            ((HeaderAwareSecurityContext) securityContext).setHeaders(containerRequest.getHeaders());
        }
        for (Map.Entry<String, String> headerEntry : httpRequest.headers()) {
            incomingHeaders.add(headerEntry.getKey(), headerEntry.getValue());
        }

        // for HTTP 1.0 we always close the connection after the request, for 1.1 we look at the Connection header
        boolean closeConnection = protocolVersion == HttpVersion.HTTP_1_0;
        final String connectionHeader = HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONNECTION);
        if (connectionHeader != null && connectionHeader.equals("close")) {
            closeConnection = true;
        }
        containerRequest.setWriter(new NettyResponseWriter(protocolVersion,
                closeConnection, e.getChannel()));

        // see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html, sec 14.18 Date.
        final Date responseDate = new Date();
        containerRequest.getHeaders().add(HttpHeaders.Names.DATE, HttpDateFormat.getPreferedDateFormat().format(responseDate));
        appHandler.handle(containerRequest);

        // *sigh*, netty has a list of Map.Entry and jersey wants a map. :/
        final MultivaluedMap<String, String> headers = containerRequest.getHeaders();
        for (Map.Entry<String, String> header : httpRequest.headers()) {
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
        final Channel channel = ctx.getChannel();
        if (e instanceof ClosedChannelException || channel == null || !channel.isOpen()) {
            log.debug("Not writing any response, channel is already closed.", e.getCause());
            return;
        }
        log.error("Uncaught exception during jersey resource handling", e.getCause());
        final HttpRequest request = (HttpRequest) ctx.getAttachment();
        final HttpVersion protocolVersion;
        if (request != null && request.getProtocolVersion() != null) {
            protocolVersion = request.getProtocolVersion();
        } else {
            protocolVersion = HttpVersion.HTTP_1_0;
        }

        final DefaultHttpResponse response = new DefaultHttpResponse(protocolVersion, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        new ChannelBufferOutputStream(buffer).writeBytes(e.toString());
        response.setContent(buffer);

        final ChannelFuture channelFuture = channel.write(response);

        if ((protocolVersion == HttpVersion.HTTP_1_0)
                || request == null
                || HttpHeaders.getHeader(request, HttpHeaders.Names.CONNECTION).equalsIgnoreCase("close")) {
            channelFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public void invalidRequestSent(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Channel channel = ctx.getChannel();
        if (channel == null || !channel.isOpen()) {
            log.debug("Not writing any response, channel is already closed.", e.getCause());
            return;
        }

        final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.BAD_REQUEST);
        response.headers().add(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
        response.headers().add(HttpHeaders.Names.CONNECTION, "close");
        final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        new ChannelBufferOutputStream(buffer).writeBytes("Your client has sent a malformed or illegal request.\n");
        response.setContent(buffer);

        final ChannelFuture channelFuture = channel.write(response);

        channelFuture.addListener(ChannelFutureListener.CLOSE);
    }

}
