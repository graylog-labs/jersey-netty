package org.graylog2.jersey.container.netty;

import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.spi.ContainerProvider;

import javax.ws.rs.ProcessingException;

public class NettyContainerProvider implements ContainerProvider {
    @Override
    public <T> T createContainer(Class<T> type, ApplicationHandler appHandler) throws ProcessingException {
        if (type != NettyContainer.class) {
            return null;
        }
        return type.cast(new NettyContainer(appHandler));
    }
}
