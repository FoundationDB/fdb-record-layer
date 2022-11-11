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
import com.apple.foundationdb.relational.grpc.GrpcConstants;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.server.jdbc.v1.JDBCService;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Relational Server.
 * Hosts the JDBC GRPC Service.
 */
// TODO: NEXT. Exceptions over RPC. Currently they do not come out nicely.
// TODO: Fix the JDBCRelationalStatement on the jdbc-side. Currently they let out RExceptions (because they implement
// RelationalStatement. I don't think we want this. Discuss.'
// TODO: Allow setting schema and option on connect to database as in jdbc:relational://localhost:1234/DATABASE?schema=XYZ&option=NONE, etc.
// TODO: Add remote 'safe' shutdown of server (or via signal?).
// Revisit signal handling (to load config and to do 'safe' shutdown?)
// It looks like CTRL-C is caught and we run the shutdown handler. What else is caught?
// https://www.programcreek.com/java-api-examples/?api=sun.misc.SignalHandler
// https://dzone.com/articles/basics-signal-handling
// https://github.com/dmarkwat/java8-signalhandler
// https://www.dclausen.net/javahacks/signal.html
// Clients https://github.com/ktr0731/evans and grpc-client-cli or the c++ grpc grpc-cli
// which use the reflections service.
// TODO: Read config from json/yaml file: e.g:  Get default features file from classpath with:
//  return Server.class.getResource("server.json");
// An example reading config as json
// https://github.com/grpc/grpc-java/blob/b118e00cf99c26da4665257ddcf11e666f6912b6/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideUtil.java#L51
// 
// TODO: Update health service state as we go; i.e. make it 'live'.
@SuppressWarnings({"PMD.SystemPrintln", "PMD.DoNotCallSystemExit"})
public class RelationalServer implements Closeable {
    // GRPC uses JUL.
    private static final Logger logger = Logger.getLogger(RelationalServer.class.getName());

    private Server server;
    private final int port;
    private FRL frl;

    public RelationalServer() {
        this(GrpcConstants.DEFAULT_SERVER_PORT);
    }

    public RelationalServer(int port) {
        this.port = port;
    }

    /**
     * Server port.
     * Error if you call this method before {@link #start()} completes.
     * @return The port this server is listening on
     */
    public int getPort() {
        return this.server.getPort();
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
        healthStatusManager.setStatus(HealthGrpc.SERVICE_NAME,
                HealthCheckResponse.ServingStatus.SERVING);
        healthStatusManager.setStatus(ServerReflectionGrpc.SERVICE_NAME,
                HealthCheckResponse.ServingStatus.SERVING);
        healthStatusManager.setStatus(JDBCServiceGrpc.SERVICE_NAME,
                HealthCheckResponse.ServingStatus.SERVING);
        // Build Server with Services.
        this.server = ServerBuilder.forPort(port)
                .addService(healthStatusManager.getHealthService())
                .addService(ProtoReflectionService.newInstance())
                .addService(new JDBCService(frl))
                .build();
        this.server.start();
        String services = this.server.getServices().stream()
                .map(p -> p.getServiceDescriptor().getName())
                .collect(Collectors.joining(", "));
        int port = this.server.getPort();
        logger.info(() -> "Started on port=" + port + " with services: " + services);
        // From https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/routeguide/RouteGuideServer.java
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println(Instant.now() + " Waiting on Server termination");
                try {
                    RelationalServer.this.server.shutdown();
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
            if (this.server != null) {
                // Server waits here while working.
                this.server.awaitTermination();
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
        if (this.server != null) {
            this.server.shutdown();
            awaitTermination();
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = new Options();
        Option help = new Option("h", "help", false, "Output this help message.");
        options.addOption(help);
        // Has to be a 'Number.class', see https://stackoverflow.com/questions/5585634/apache-commons-cli-option-type-and-default-value
        Option port = Option.builder().option("port").longOpt("port").hasArg(true).type(Number.class)
                .desc("Port to listen on; default=" + GrpcConstants.DEFAULT_SERVER_PORT + ".").build();
        options.addOption(port);
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
            formatter.printHelp("Main", options, true);
            return;
        }
        int serverPort = GrpcConstants.DEFAULT_SERVER_PORT;
        if (cli.hasOption(port.getOpt())) {
            try {
                serverPort = ((Number) cli.getParsedOptionValue(port.getOpt())).intValue();
            } catch (ParseException pe) {
                System.err.println("Parse failed, option=" + port.getOpt() + ". " +
                        pe.getMessage());
                System.exit(1);
            }
        }

        new RelationalServer(serverPort).start().awaitTermination();
    }
}
