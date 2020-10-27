# ZetaSQL Helper for BigQuery Utils

This directory contains both the server and client of ZetaSQL Helper. Its server imports ZetaSQL as 
dependency and implements helper functions on top of it. The server and client communicate through RPC
service (gRPC). The server can be put into a Docker image, so it is OS independent.

This project is a prerequisite of the `auto_query_fixer` in the parent directory. Please make sure you have
installed this ZetaSQL Helper Docker image or downloaded from a Docker repository before using the query fixer.

## Prerequisite
This project requires `bazel` and `docker` on your machine. The version of `bazel` is 1.0.0. The `c++` standard
is c++1z.

If you haven't installed them, please refer to their documentations: [Bazel](https://docs.bazel.build/versions/master/install.html)
 and [Docker](https://docs.docker.com/get-docker/).

## Build the C++ server and its image.

If you hope to start the ZetaSQL Helper server locally, run this command:
```bash
bazel run //zetasql_helper/local_service:run_server
```

If you hope to build the server docker image, use the following command.

```bash
docker build . -t <docker-image-name>:<version>
```

For example, the current image stored in `gcr` is built as:

```bash
docker build . -t gcr.io/sql-gravity-internship/zetasql-helper:1.0
```



## Build java client

To build a light-weighted client jar
```bash
bazel build //java/com/google/bigquery/utils/zetasqlhelper:zetasql_helper_client
```

The built jar exists in `./bazel-bin/java/com/google/bigquery/utils/zetasqlhelper/zetasql_helper_client.jar`.

Copy the jar to your project. The client jar needs to be used with external dependencies. 
Here is how to use it in Gradle:

```groovy
// Import the jar locally
implementation name: 'zetasql_helper_client'

// gRPC dependency used to set up RPC service
def JAVA_GRPC_VERSION = "1.18.0"
implementation "io.grpc:grpc-netty-shaded:" + JAVA_GRPC_VERSION
implementation "io.grpc:grpc-protobuf:" + JAVA_GRPC_VERSION
implementation "io.grpc:grpc-stub:" + JAVA_GRPC_VERSION

//docker-java used by the ZetaSQL Helper client
compile group: 'com.github.docker-java', name: 'docker-java-core', version: '3.2.5'
compile group: 'com.github.docker-java', name: 'docker-java-transport-httpclient5', version: '3.2.5'
```

Since `1.18.0` is a relatively old version, it may be overridden by a newer version when other dependencies
use the newer version of `gRPC` component in Gradle. Therefore, please try to increase `JAVA_GRPC_VERSION`
if an error pops out. `1.30.0` has been verified to be compatible with the client.

### Sample Usage

When you import the jar inside your project, you could call the RPC functions like this:
```java
QueryFunctionRange range =
        ZetaSqlHelper.extractFunctionRange(query, row, column);
```

You don't have to set up any connections to a server because the client will automatically check if a docker image 
has started. If not, the client will pull the image, run it, and expose the port 50051. 

Of course you could manually specify the docker image and the connection port like this:

```java
LocalServiceDockerProvider.setImage("your own ZetaSQL Helper Image");
int yourPort = 50052;
Client.setPort(yourPort);

// call the ZetaSQL Helper RPC functions
```

### Caution with Docker
Please make sure your program has enough permission to use docker. Before you start your program
with the ZetaSQL Helper client, try `docker ps` and see if the current user has the permission to use docker.
If not, please switch to a user or a group with enough permission.

## What functions does it provide and how do I add my own?
Please go to `zetasql_helper/local_service/local_service_rgrpc.h` to check all the RPC functions. Also you
could check their client caller in `java/com/google/bigquery/utils/zetasqlhelper/ZetaSqlHelper.java`. 

If you would like to develop your own RPC functions inside, please first add your RPC definition inside 
`local_service.proto` at `zetasql_helper/local_service`. Then implement these methods in both the server
and client mentioned above.
