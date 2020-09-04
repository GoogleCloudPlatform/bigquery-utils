package com.google.bigquery.utils.zetasqlhelper;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import java.util.Collections;
import java.util.List;

/**
 * A controller responsible for managing the Docker Image of ZetaSQL Helper server. It can check
 * if a server container is started, start a server and connect to a server.
 */
public class LocalServiceDockerProvider implements LocalServiceProvider {

    private final static Integer SERVER_PORT = 50051;
    private final static String LOCAL_HOST = "localhost:";
    // The ZetaSQL Helper image  can be modified using setImage method.
    private static String IMAGE = "gcr.io/sql-gravity-internship/zetasql-helper:1.0";

    private final static String RUNNING = "running";
    // Waiting time for starting server. If the server container can not start in this period,
    // it will be considered shutdown. Unit is millisecond.
    private final static long START_SERVER_TIMEOUT = 5000;

    private final DockerClient client;

    public LocalServiceDockerProvider() {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();

        client = DockerClientImpl.getInstance(config, httpClient);
    }

    /**
     * Start a server locally at the given port number.
     *
     * @param port port number
     */
    @Override
    public void startServer(final Integer port) {
        String binding = String.format("%d:%d", port, SERVER_PORT);
        CreateContainerResponse response =
                client
                        .createContainerCmd(IMAGE)
                        .withExposedPorts(ExposedPort.parse("" + SERVER_PORT))
                        .withHostConfig(
                                HostConfig.newHostConfig().withPortBindings(PortBinding.parse(binding)))
                        .exec();

        String containerId = response.getId();
        client.startContainerCmd(containerId).exec();

        if (!waitServerToStart(containerId)) {
            throw new LocalServiceException("Unable to start ZetaSql Helper Service Docker container");
        }
    }

    /**
     * Check if a server has been started at the given port number.
     *
     * @param port port number
     * @return boolean of whether a server is on.
     */
    @Override
    public boolean isServerOn(final Integer port) {
        List<Container> containers = client.listContainersCmd()
                .withAncestorFilter(Collections.singletonList(IMAGE))
                .exec();

        for (Container container : containers) {
            ContainerPort[] containerPorts = container.getPorts();
            for (ContainerPort containerPort : containerPorts) {
                if (SERVER_PORT.equals(containerPort.getPrivatePort()) && port.equals(containerPort.getPublicPort())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Connect to the server at the given port number.
     *
     * @param port port number
     * @return Channel to the server
     */
    @Override
    public Channel connect(final Integer port) {
        try {
            // If the server is not active, try to start it.
            if (!isServerOn(port)) {
                startServer(port);
            }
        } catch (RuntimeException exception) {
            throw new LocalServiceException("unable to create a channel to ZetaSql Helper Service through Docker", exception);
        }

        return ManagedChannelBuilder.forTarget(LOCAL_HOST + port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();
    }

    private boolean waitServerToStart(String ContainerId) {
        long now = System.currentTimeMillis();
        long endTime = now + LocalServiceDockerProvider.START_SERVER_TIMEOUT;

        for (; now < endTime; now = System.currentTimeMillis()) {
            List<Container> containers = client.listContainersCmd()
                    .withIdFilter(Collections.singletonList(ContainerId))
                    .exec();
            if (containers.size() == 0) {
                continue;
            }
            if (containers.get(0).getState().equals(RUNNING)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Set the ZetaSQL Helper image. Please set the image before calling the RPC.
     * @param image user-defined ZetaSQL Helper image
     */
    public static void setImage(String image) {
        IMAGE = image;
    }

    public static String getImage() {
        return IMAGE;
    }

}
