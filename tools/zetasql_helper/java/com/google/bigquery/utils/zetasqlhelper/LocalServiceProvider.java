package com.google.bigquery.utils.zetasqlhelper;

import io.grpc.Channel;

import java.util.ServiceLoader;

/**
 * Interface to provide ZetaSQL Helper service.
 */
public interface LocalServiceProvider {

    void startServer(Integer port);

    boolean isServerOn(Integer port);

    Channel connect(Integer port);


    /**
     * User a service loader to provide the available ZetaSQL Helper service.
     *
     * @param port port number to host the service.
     * @return Channel to the ZetaSQL Helper server.
     */
    static Channel loadChannel(Integer port) {
        for (LocalServiceProvider provider : ServiceLoader.load(LocalServiceProvider.class)) {
            return provider.connect(port);
        }
        throw new IllegalStateException("No ZetaSQL Helper LocalServiceProvider loaded.");
    }
}
