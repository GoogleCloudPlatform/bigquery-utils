package com.google.bigquery.utils.zetasqlhelper;

import io.grpc.Channel;

/**
 * Client to establish connections with ZetaSQL Helper server.
 */
public class Client {

    private static ZetaSqlHelperLocalServiceGrpc.ZetaSqlHelperLocalServiceBlockingStub stub;
    // The port number can be modified using setPort method.
    private static int port = 50051;

    private Client() {
    }

    public static ZetaSqlHelperLocalServiceGrpc.ZetaSqlHelperLocalServiceBlockingStub getStub() {
        if (stub == null) {
            Channel channel = getChannel();
            stub = ZetaSqlHelperLocalServiceGrpc.newBlockingStub(channel);
        }
        return stub;
    }

    private static Channel getChannel() {
        return LocalServiceProvider.loadChannel(port);
    }

    public static int getPort() {
        return port;
    }

    /**
     * Set the port number to connect to the ZetaSQL Helper server. The default number is 50051.
     *
     * @param port user-defined port number
     */
    public static void setPort(int port) {
        Client.port = port;
    }
}
