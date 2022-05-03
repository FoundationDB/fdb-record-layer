/*
 * EmbeddedRelationalEngine.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.CatalogQueryFactory;
import com.apple.foundationdb.relational.api.ddl.ConstantActionFactory;
import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.StorageCluster;

import java.util.Collection;
import java.util.List;

/**
 * The Engine that drives and manages an Embedded Relational instance.
 */
public class EmbeddedRelationalEngine {
    private final List<StorageCluster> clusters;

    private final EmbeddedRelationalDriver driver;
    private final ConstantActionFactory constantActionFactory;
    private final DdlQueryFactory queryFactory;

    //TODO(bfines) eventually we need to move StoreCatalog into StorageCluster
    public EmbeddedRelationalEngine(List<StorageCluster> fdbClusters,
                                  ConstantActionFactory constantActionFactory,
                                  StoreCatalog storeCatalog,
                                  SchemaTemplateCatalog templateCatalog) {
        if (fdbClusters.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one FDB cluster to connect to");
        }
        this.clusters = fdbClusters;
        this.driver = new EmbeddedRelationalDriver(this);
        this.constantActionFactory = constantActionFactory;
        this.queryFactory = new CatalogQueryFactory(storeCatalog, templateCatalog);
    }

    public void registerDriver() throws RelationalException {
        Relational.registerDriver(driver);
    }

    public void deregisterDriver() throws RelationalException {
        Relational.deregisterDriver(driver);
    }

    public Collection<StorageCluster> getStorageClusters() {
        return clusters;
    }

    public DdlConnection getDdlConnection() {
        //TODO(bfines) this won't work for multiple clusters
        return new DdlConnection(clusters.get(0).getTransactionManager(), constantActionFactory, queryFactory);
    }

}
