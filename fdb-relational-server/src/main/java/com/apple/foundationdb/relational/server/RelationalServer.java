/*
 * RelationalServer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.relational.server;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcConstants;
import com.apple.foundationdb.relational.jdbc.grpc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.server.jdbc.v1.JDBCService;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Relational Server.
 * Hosts the JDBC GRPC Service and an HTTP server to export metrics on.
 * (On why two ports in one server, see prometheus issue for discussion
 * https://github.com/prometheus/prometheus/issues/8414)
 * @see InProcessRelationalServer
 */
// Exceptions are ongoing work. SQLException 'works' now. Polish. Other exceptions need to be figured and handled.
// TODO: Read config from json/yaml file: e.g:  Get default features file from classpath with:
//  return Server.class.getResource("server.json");
//  Add config. on whether to record histograms or just cheap metrics as configurable item.
// An example reading config as json
// https://github.com/grpc/grpc-java/blob/b118e00cf99c26da4665257ddcf11e666f6912b6/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideUtil.java#L51
// 
// We now have an httpserver for metrics; could use it for REST commands and exposing current configuration.
// TODO: Update health service state as we go; i.e. make it 'live'.
@SuppressWarnings({"PMD.SystemPrintln", "PMD.DoNotCallSystemExit"})
public class RelationalServer implements Closeable {
    // GRPC uses JUL.
    private static final Logger logger = LogManager.getLogger(RelationalServer.class.getName());
    private static final int DEFAULT_HTTP_PORT = GrpcConstants.DEFAULT_SERVER_PORT + 1;

    private Server grpcServer;
    private final int grpcPort;
    private final int httpPort;
    private FRL frl;
    private final CollectorRegistry collectorRegistry;

    // Visible for the test fixture only so it can pass a CollectorRegistry.
    @VisibleForTesting
    RelationalServer(int grpcPort, int httpPort, CollectorRegistry collectorRegistry) {
        this.grpcPort = grpcPort;
        this.httpPort = httpPort;
        this.collectorRegistry = collectorRegistry;
    }

    public RelationalServer(int grpcPort, int httpPort) {
        this(grpcPort, httpPort, CollectorRegistry.defaultRegistry);
    }

    @Override
    public String toString() {
        return "listening=" + this.grpcServer.getListenSockets() +
                ", services=" + this.grpcServer.getServices().stream().map(m -> m.getServiceDescriptor().getName())
                .collect(Collectors.toList()) +
                ", httpPort=" + this.httpPort;
    }

    /**
     * GRPC port.
     * Error if you call this method before {@link #start()} completes.
     * @return The port GRPC is listening on
     */
    public int getGrpcPort() {
        return this.grpcServer.getPort();
    }

    /**
     * HTTP port.
     * Error if you call this method before {@link #start()} completes.
     * @return The port HTTP is listening on
     */
    public int getHttpPort() {
        return this.httpPort;
    }

    @VisibleForTesting
    CollectorRegistry getCollectorRegistry() {
        return this.collectorRegistry;
    }

    RelationalServer start() throws IOException {
        // Create access to backing database.
        // TODO: Make this multi-query/-tenant/-database!
        FRL frl;
        try {
            frl = new FRL();
        } catch (RelationalException ve) {
            throw new IOException(ve);
        }
        HealthStatusManager healthStatusManager = new HealthStatusManager();
        // Get SERVICE_NAME from the wrapping from the generated Grpc class.
        healthStatusManager.setStatus(HealthGrpc.SERVICE_NAME, HealthCheckResponse.ServingStatus.SERVING);
        healthStatusManager.setStatus(ServerReflectionGrpc.SERVICE_NAME, HealthCheckResponse.ServingStatus.SERVING);
        healthStatusManager.setStatus(JDBCServiceGrpc.SERVICE_NAME, HealthCheckResponse.ServingStatus.SERVING);
        // For now, collect cheap metrics only. Later can do all/'expensive'. Metrics are recorded on the default
        // prometheus collector so can be added-to.
        var grpcMetrics = MonitoringServerInterceptor.create(Configuration.allMetrics()
                .withCollectorRegistry(this.collectorRegistry));
        // Build Server with Services.
        // We install TransmitStatusRuntimeExceptionInterceptor as a server interceptor to catch
        // StatusRuntimeExceptions below. It closes the connection and will return actual Status instead
        // of the default UNKNOWN with no detail. There may be performance implications running with
        // this interceptor -- see its internal serializing wrapper class. TODO.
        // A general server-side catch is a tricky business. One approach might be customizing a version of
        // TransmitStatusRuntimeException to catch all RuntimeExceptions and then do as we do over in
        // JDBCService in handleUncaughtException. For now, catch RuntimeExceptions in Services and
        // convert to StatusRuntimeException. At least this way we can get the client client some detail.
        // Server exception handling and how they manifest on client-side is still a work-in-progress.
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(ServerInterceptors.intercept(healthStatusManager.getHealthService(), grpcMetrics))
                .addService(ServerInterceptors.intercept(ProtoReflectionService.newInstance(), grpcMetrics))
                .addService(ServerInterceptors.intercept(new JDBCService(frl), grpcMetrics))
                .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
                .build();
        this.grpcServer.start();
        String services = this.grpcServer.getServices().stream()
                .map(p -> p.getServiceDescriptor().getName())
                .collect(Collectors.joining(", "));
        // Start http server in daemon mode.
        new HTTPServer.Builder().withPort(this.httpPort).withRegistry(this.collectorRegistry).build();
        if (logger.isInfoEnabled()) {
            logger.info("Started on grpcPort={}/httpPort={} with services: {}",
                    getGrpcPort(), getHttpPort(), services);
        }
        // From https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideServer.java
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println(Instant.now() + " Waiting on Server termination");
                try {
                    RelationalServer.this.grpcServer.shutdown();
                    RelationalServer.this.awaitTermination();
                } catch (InterruptedIOException e) {
                    throw new RuntimeException(e);
                }
                System.err.println(Instant.now() + " Server shutdown");
            }
        });
        // This is what is used on the cli. It is overkill for here but easy for now.
        return this;
    }

    void awaitTermination() throws InterruptedIOException {
        try {
            if (this.grpcServer != null) {
                // Server waits here while working.
                this.grpcServer.awaitTermination();
            }
        } catch (InterruptedException ioe) {
            InterruptedIOException iioe = new InterruptedIOException();
            iioe.initCause(ioe);
            throw iioe;
        }
    }

    @Override
    public void close() throws IOException {
        IOException ioe = null;
        if (this.frl != null) {
            try {
                this.frl.close();
            } catch (IOException e) {
                ioe = e;
            } catch (Exception e) {
                ioe = new IOException(e);
            }
        }
        if (this.grpcServer != null) {
            this.grpcServer.shutdown();
            awaitTermination();
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    /**
     * Process port option.
     */
    private static int getPort(CommandLine cli, Option option, int defaultPort) {
        int port = defaultPort;
        if (cli.hasOption(option.getOpt())) {
            try {
                port = ((Number) cli.getParsedOptionValue(option.getOpt())).intValue();
            } catch (ParseException pe) {
                System.err.println("Parse failed, option=" + option.getOpt() + ". " +
                        pe.getMessage());
                System.exit(1);
            }
        }
        return port;
    }

    @ExcludeFromJacocoGeneratedReport
    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = new Options();
        Option help = new Option("h", "help", false, "Output this help message.");
        options.addOption(help);
        // Has to be a 'Number.class', see https://stackoverflow.com/questions/5585634/apache-commons-cli-option-type-and-default-value
        Option grpcPortOption = Option.builder().option("g").longOpt("grpcPort").hasArg(true).type(Number.class)
                .desc("Port for GRPC to listen on; default=" + GrpcConstants.DEFAULT_SERVER_PORT + ".").build();
        options.addOption(grpcPortOption);
        Option httpPortOption = Option.builder().option("p").longOpt("httpPort").hasArg(true).type(Number.class)
                .desc("Port for HTTP to listen on; default=" + DEFAULT_HTTP_PORT + ".").build();
        options.addOption(httpPortOption);
        CommandLineParser parser = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException pe) {
            System.err.println("Parse of command-line failed: " + pe.getMessage());
            System.exit(1);
        }
        if (cli.hasOption(help.getOpt())) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("relational", options, true);
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info("FDB_CLUSTER_FILE: " + System.getenv("FDB_CLUSTER_FILE"));
            logger.info("DYLD_LIBRARY_PATH: " + System.getenv("DYLD_LIBRARY_PATH"));
        }
        int grpcPort = getPort(cli, grpcPortOption, GrpcConstants.DEFAULT_SERVER_PORT);
        int httpPort = getPort(cli, httpPortOption, DEFAULT_HTTP_PORT);
        new RelationalServer(grpcPort, httpPort).start().awaitTermination();
    }
}
