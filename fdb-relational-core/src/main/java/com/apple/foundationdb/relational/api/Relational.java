/*
 * Relational.java
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.concurrent.CopyOnWriteArrayList;

public class Relational {
    private static final CopyOnWriteArrayList<RelationalDriver> registeredDrivers = new CopyOnWriteArrayList<>();

    public static DatabaseConnection connect(@Nonnull URI url,@Nonnull Options connectionOptions){
        return connect(url,null,connectionOptions);
    }

    public static DatabaseConnection connect(@Nonnull URI url, @Nullable Transaction existingTransaction, @Nonnull Options connectionOptions){
        //all connection URLs should start with "rlsc" to represent "relational layer service connection",
        // so we strip that out from the URI and pass the remainder in
        String scheme = url.getScheme();
        if(!"rlsc".equalsIgnoreCase(scheme)){
            throw new RelationalException("Unable to connect to url <"+url+">: invalid scheme <"+scheme+">", RelationalException.ErrorCode.INVALID_PATH);
        }
        URI nonSchemeURI = URI.create(url.toString().substring(5));
        return getDriver(nonSchemeURI).connect(nonSchemeURI,existingTransaction,connectionOptions);
    }

    public static RelationalDriver getDriver(@Nonnull URI connectionUrl){
        for(RelationalDriver driver: registeredDrivers){
            if(driver.acceptsURL(connectionUrl)){
                return driver;
            }
        }
        throw new RelationalException("No Driver registered which can interpret scheme <"+connectionUrl.getScheme()+">", RelationalException.ErrorCode.UNKNOWN_SCHEME);
    }

    public static void registerDriver(@Nonnull RelationalDriver newDriver){
        registeredDrivers.add(newDriver);
    }

    public static void deregisterDriver(@Nonnull RelationalDriver driver) {
        registeredDrivers.remove(driver);
    }
}
