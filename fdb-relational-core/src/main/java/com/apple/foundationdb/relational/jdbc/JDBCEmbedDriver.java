/*
 * JDBCEmbedDriver.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.CatalogMetaData;
import com.apple.foundationdb.relational.recordlayer.DirectFdbConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RecordLayerEngine;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.QueryLogger;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.util.BuildVersion;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 * JDBC Driver that does a direct-connect to a Relational engine/instance.
 * Handles the 'jdbc:embed:' JDBC URL.
 * Hosts a {@link com.apple.foundationdb.relational.api.EmbeddedRelationalDriver} adding bare-bones JDBC functionality.
 */
// {@link com.apple.foundationdb.relational.api.EmbeddedRelationalDriver} is almost a JDBC Driver
// but is missing key functionality such as registration with the JDBC DriverManager.
// We introduce this new class for fielding JDBC Driver needs as backfilling
// into {@link com.apple.foundationdb.relational.api.EmbeddedRelationalDriver} will break
// how this 'embedded' driver is currently consumed (e.g. being able to register/deregister,
// passing custom engine instances to the embedded class on construction, etc.).
@SuppressWarnings({"PMD.SystemPrintln"})
public final class JDBCEmbedDriver implements java.sql.Driver {
    public static final String JDBC_COLON = "jdbc:";
    public static final String JDBC_URL_PREFIX = JDBC_COLON + "embed:";

    // Register this driver with the DriverManager on classloading.
    // From Code Example 9-1 in the JDBC 4.3 spec.
    static {
        try {
            DriverManager.registerDriver(new JDBCEmbedDriver());
        } catch (SQLException sqlException) {
            System.err.println(sqlException);
        }
    }

    private final EmbeddedRelationalDriver embeddedRelationalDriver;
    private final StoreCatalog storeCatalog;

    /**
     * Public so serviceloader can load our driver.
     * Used when we register this Driver as part of classloading.
     * See the static block at the head of this class.
     * The below is taken from DbState in the CLI. fdb 7.1.28 seems to work better than
     * default 7.1.4 when it comes to finding shard libs and default fdb config locations.
     */
    // TODO: Read driver configuration from properties or environment and/or JDBC URL.
    // Bulk of below is copied from DbState.
    public JDBCEmbedDriver() {
        FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
        DirectFdbConnection fdbConnection = new DirectFdbConnection(fdbDb, NoOpMetricRegistry.INSTANCE);
        RelationalKeyspaceProvider.registerDomainIfNotExists("FRL");
        KeySpace keySpace = RelationalKeyspaceProvider.getKeySpace();
        try (Transaction txn = fdbConnection.getTransactionManager().createTransaction(Options.NONE)) {
            this.storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        } catch (RelationalException e) {
            throw new RuntimeException(e);
        }
        RecordLayerConfig rlConfig = RecordLayerConfig.getDefault();
        RecordLayerMetadataOperationsFactory ddlFactory = new RecordLayerMetadataOperationsFactory.Builder()
                .setRlConfig(rlConfig)
                .setBaseKeySpace(keySpace)
                .setStoreCatalog(this.storeCatalog).build();
        QueryLogger.configure(Options.NONE);
        this.embeddedRelationalDriver = new EmbeddedRelationalDriver(RecordLayerEngine.makeEngine(
                rlConfig,
                Collections.singletonList(fdbDb),
                keySpace,
                storeCatalog,
                NoOpMetricRegistry.INSTANCE,
                ddlFactory,
                RelationalPlanCache.buildWithDefaults()));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return this.embeddedRelationalDriver.acceptsURL(URI.create(url.substring(JDBC_COLON.length())));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return BuildVersion.getInstance().getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return BuildVersion.getInstance().getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Not implemented");
    }

    @Override
    // The below is ugly at least for now. We return versions of core classes
    // and interfaces with minimal customization. TODO: Come back and make
    // implementations after more direction on how we want to access core (e.g.
    // keep using the EmbeddedRelationalDriver or force clients to come in via JDBC,
    // etc.)
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("Not an acceptable relational JDBC url: " + url);
        }
        try {
            RelationalConnection relationalConnection =
                    this.embeddedRelationalDriver.connect(URI.create(url.substring(JDBC_COLON.length())),
                            null, Options.NONE);
            final EmbeddedRelationalConnection delegate =
                    (EmbeddedRelationalConnection) relationalConnection.unwrap(EmbeddedRelationalConnection.class);
            return new RelationalConnection() {
                private static final String DRIVER_NAME = "Relational Embedded/Local JDBC Driver";
                /**
                 * Chose a transaction level that we *pretend* to support (Rather than throw an exception that stops
                 * all processing).
                 * TODO: Implement.
                 */
                private static final int DEFAULT_TRANSACTION_LEVEL = Connection.TRANSACTION_REPEATABLE_READ;
                @ExcludeFromJacocoGeneratedReport
                @Override
                public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
                    return delegate.createStatement(resultSetType, resultSetConcurrency);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
                    return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
                    return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
                    return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
                    return delegate.prepareStatement(sql, autoGeneratedKeys);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
                    return delegate.prepareStatement(sql, columnIndexes);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
                    return delegate.prepareStatement(sql, columnNames);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public CallableStatement prepareCall(String sql) throws SQLException {
                    return delegate.prepareCall(sql);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
                    return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
                    return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public String nativeSQL(String sql) throws SQLException {
                    return delegate.nativeSQL(sql);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void abort(Executor executor) throws SQLException {
                    delegate.abort(executor);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
                    delegate.setNetworkTimeout(executor, milliseconds);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public int getNetworkTimeout() throws SQLException {
                    return delegate.getNetworkTimeout();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setReadOnly(boolean readOnly) throws SQLException {
                    // Do nothing. Swallow rather than return useless complaint about not being implemented
                    // which stops all processing.
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public boolean isReadOnly() throws SQLException {
                    return delegate.isReadOnly();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setCatalog(String catalog) throws SQLException {
                    delegate.setCatalog(catalog);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public String getCatalog() throws SQLException {
                    return delegate.getCatalog();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setTransactionIsolation(int level) throws SQLException {
                    if (getMetaData().getDefaultTransactionIsolation() != level) {
                        throw new SQLFeatureNotSupportedException("TransactionIsolation level=" + level);
                    }
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public int getTransactionIsolation() throws SQLException {
                    return delegate.getTransactionIsolation();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public SQLWarning getWarnings() throws SQLException {
                    // Return null for now until we implement Warnings. Returning an exception breaks processing.kk
                    return null;
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void clearWarnings() throws SQLException {
                    delegate.clearWarnings();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Map<String, Class<?>> getTypeMap() throws SQLException {
                    return delegate.getTypeMap();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
                    delegate.setTypeMap(map);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setHoldability(int holdability) throws SQLException {
                    delegate.setHoldability(holdability);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public int getHoldability() throws SQLException {
                    return delegate.getHoldability();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Savepoint setSavepoint() throws SQLException {
                    return delegate.setSavepoint();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Savepoint setSavepoint(String name) throws SQLException {
                    return delegate.setSavepoint(name);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void rollback(Savepoint savepoint) throws SQLException {
                    delegate.rollback(savepoint);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void releaseSavepoint(Savepoint savepoint) throws SQLException {
                    delegate.releaseSavepoint(savepoint);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Clob createClob() throws SQLException {
                    return delegate.createClob();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Blob createBlob() throws SQLException {
                    return delegate.createBlob();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public NClob createNClob() throws SQLException {
                    return delegate.createNClob();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public SQLXML createSQLXML() throws SQLException {
                    return delegate.createSQLXML();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public boolean isValid(int timeout) throws SQLException {
                    return delegate.isValid(timeout);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setClientInfo(String name, String value) throws SQLClientInfoException {
                    delegate.setClientInfo(name, value);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void setClientInfo(Properties properties) throws SQLClientInfoException {
                    delegate.setClientInfo(properties);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public String getClientInfo(String name) throws SQLException {
                    return delegate.getClientInfo(name);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Properties getClientInfo() throws SQLException {
                    return delegate.getClientInfo();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
                    return delegate.createArrayOf(typeName, elements);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
                    return delegate.createStruct(typeName, attributes);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public <T> T unwrap(Class<T> iface) throws SQLException {
                    return delegate.unwrap(iface);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public boolean isWrapperFor(Class<?> iface) throws SQLException {
                    return delegate.isWrapperFor(iface);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public RelationalStatement createStatement() throws SQLException {
                    return delegate.createStatement();
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
                    return delegate.prepareStatement(sql);
                }

                @ExcludeFromJacocoGeneratedReport
                @Override
                public void beginTransaction() throws SQLException {

                }

                @Nonnull
                @Override
                @ExcludeFromJacocoGeneratedReport
                public Options getOptions() {
                    return delegate.getOptions();
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public void setOption(Options.Name name, Object value) throws SQLException {
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public URI getPath() {
                    return null;
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public void setAutoCommit(boolean autoCommit) throws SQLException {
                    delegate.setAutoCommit(autoCommit);
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public boolean getAutoCommit() throws SQLException {
                    return delegate.getAutoCommit();
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public void commit() throws SQLException {
                    delegate.commit();
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public void rollback() throws SQLException {
                    delegate.rollback();
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public void close() throws SQLException {
                    delegate.close();
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public boolean isClosed() throws SQLException {
                    return delegate.isClosed();
                }

                @Override
                public DatabaseMetaData getMetaData() throws SQLException {
                    // Return subclass of CatalogMetaData if CATALOG context else, a bare-bones metadata.
                    String schema = delegate.getSchema();
                    if (schema != null &&  schema.equalsIgnoreCase(RelationalKeyspaceProvider.CATALOG)) {
                        return new CatalogMetaData(delegate, storeCatalog) {
                            @Override
                            public String getDriverName() throws SQLException {
                                return DRIVER_NAME;
                            }

                            @Override
                            public int getDefaultTransactionIsolation() throws SQLException {
                                return DEFAULT_TRANSACTION_LEVEL;
                            }

                            @Override
                            public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
                                return getDefaultTransactionIsolation() == level;
                            }

                            @Override
                            public String getSQLKeywords() throws SQLException {
                                return "";
                            }

                            @Override
                            public String getCatalogTerm() throws SQLException {
                                return RelationalKeyspaceProvider.CATALOG;
                            }

                            @Override
                            public String getExtraNameCharacters() throws SQLException {
                                return "";
                            }

                            @Override
                            public String getIdentifierQuoteString() throws SQLException {
                                return "\"";
                            }

                            @Override
                            public boolean supportsANSI92EntryLevelSQL() throws SQLException {
                                return true;
                            }

                            @Override
                            public boolean supportsANSI92IntermediateSQL() throws SQLException {
                                return false;
                            }
                        };
                    } else {
                        // Bare-bones metadata for non-catalog connections.
                        return new RelationalDatabaseMetaData() {
                            @Override
                            public String getDriverName() throws SQLException {
                                return DRIVER_NAME;
                            }

                            @Override
                            public int getDefaultTransactionIsolation() throws SQLException {
                                return DEFAULT_TRANSACTION_LEVEL;
                            }

                            @Override
                            public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
                                return getDefaultTransactionIsolation() == level;
                            }

                            @Override
                            public RelationalResultSet getSchemas() throws SQLException {
                                // Return this for now; an empty ResultSet.
                                return new IteratorResultSet(null, null, 0);
                            }

                            @Override
                            public RelationalResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
                                // Return this for now; an empty ResultSet.
                                return new IteratorResultSet(null, null, 0);
                            }

                            @Nonnull
                            @Override
                            public RelationalResultSet getTables(String catalog, String schema, String tableName, String[] types) throws SQLException {
                                // Return this for now; an empty ResultSet.
                                return new IteratorResultSet(null, null, 0);
                            }

                            @Nonnull
                            @Override
                            public RelationalResultSet getColumns(String catalog, String schema, String table, String column) throws SQLException {
                                // Return this for now; an empty ResultSet.
                                return new IteratorResultSet(null, null, 0);
                            }

                            @Override
                            public RelationalResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
                                return null;
                            }

                            @Override
                            public RelationalResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
                                return null;
                            }

                            @Override
                            public String getSQLKeywords() throws SQLException {
                                return "";
                            }

                            @Override
                            public String getCatalogTerm() throws SQLException {
                                return RelationalKeyspaceProvider.CATALOG;
                            }

                            @Override
                            public String getExtraNameCharacters() throws SQLException {
                                return "";
                            }

                            @Override
                            public String getIdentifierQuoteString() throws SQLException {
                                return "\"";
                            }

                            @Override
                            public boolean supportsANSI92EntryLevelSQL() throws SQLException {
                                return true;
                            }

                            @Override
                            public boolean supportsANSI92IntermediateSQL() throws SQLException {
                                return false;
                            }
                        };
                    }
                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public void setSchema(String schema) throws SQLException {

                }

                @Override
                @ExcludeFromJacocoGeneratedReport
                public String getSchema() throws SQLException {
                    return null;
                }
            };
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }
}
