/*
 * package-info.java
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

/**
 * Relational JDBC Driver
 *
 * The Relational JDBC URL starts with <code>jdbc:relational://</code>. We use the same JDBC URL format as mysql/postgres
 * JDBC drivers; i.e. <code>jdbc:relational://HOST[:PORT]</code> to specify the remote server and port. If no HOST+PORT
 * specified, then the URL refers to an `inprocess` server; i.e.. a server running in the current process which is
 * accessed directly without RPC and without serializations. The URL path can specifes the database to connect to. The
 * query string is used for passing options. For example:
 * <pre>
 *  jdbc:relational://127.0.0.1/__SYS
 *  jdbc:relational://127.0.0.1
 *  jdbc:relational://localhost/__SYS
 *  jdbc:relational://relational.apple.com:1234/__SYS
 *  jdbc:relational://relational.apple.com:1234/__SYS?schema=TMPL&amp;options=X
 *  jdbc:relational:///__SYS
 *  jdbc:relational:///__SYS?schema=TMPL&amp;options=X
 * </pre>
 *
 * <h2>Exceptions</h2>
 * Exceptions will for the most part come up out of RPC or will originate over on the server.
 * There are two types currently; handled {@link java.sql.SQLException}s and unhandled grpc
 * {@link io.grpc.StatusRuntimeException}; the latter will usually be opaque UNKNOWNs or INTERNALS at least until we
 * develop better handling.
 * <h3>Server-Side/Client-Side SQLExceptions</h3>
 * SQLExceptions thrown on the server-side will manifest on the client-side as SQLExceptions populated with the
 * originals' messages, SQLCodes, and "vendor" error code if present. The client-side type may be that of the original
 * but it may also be just a flat {@link java.sql.SQLException} if the type is not publicly accessible to the grpc
 * module managing the transforms (fdb-relational-api is NOT a dependency of fdb-relational-grpc currently so the likes of
 * ContextualSQLException are not available to the transform code; we could change this if wanted).
 * <p>The client-side * manifestation currently has no markings to indicate it a reflection of a server-side throw; its
 * presumed server-side is the origin. We could fix this easy enough; e.g. server-side originating exceptions show in
 * the client as a ServerSideOriginatingSQLException (or any other such marker).
 * <p>Do we want client-side SQLException to include <code>cause</code> if present (currently it does)? Do we want
 * the client-side to include server-side stack trace (probably not)?
 * <h3>StatusRuntimeException</h3>
 * Server- or network-originated unhandled exceptions will bubble up on the client-side as instances of
 * {@link io.grpc.StatusRuntimeException}; i.e. RuntimeExceptions (this behavior is of GRCP). We'll tamper this
 * phenomenon as we learn of the type of exceptions we'll be seeing. We've installed an interceptor for the JDBCService
 * to catch unhandled SQLExceptions and which we can enhance as we learn more about the types of problems we'll see.
 */
// Names in this package are overwrought; we are in the relational.jdbc package yet names have a JDBCRelational prefix.
// Ideally, the JDBCRelationalDriver class (com.apple.foundationdb.relational.jdbc.JDBCRelationalDriver) would be known as
// c.a.f.r.j.Driver but our spotbug rules preclude it with a complaint that
// "The class name com.apple.foundationdb.relational.jdbc.Driver shadows the simple name of implemented interface java.sql.Driver".
// The long names convey-some that these classes implement the Relational* Interfaces by prefixing the implementation class
// name with JDBC. JDBCRelationalDriver maps a little to the `jdbc:relational://` JDBC URL. They are awkward.
package com.apple.foundationdb.relational.jdbc;
