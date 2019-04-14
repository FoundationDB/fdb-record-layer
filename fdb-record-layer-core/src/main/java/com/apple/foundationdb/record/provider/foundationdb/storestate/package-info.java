/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
 * Classes for caching {@link com.apple.foundationdb.record.RecordStoreState RecordStoreState} information.
 * If not cached, every {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore FDBRecordStore}
 * needs to retrieve this information with the database prior to performing any other operations with
 * the record store. (In most cases, this should be done with the
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore.Builder#create create()} or
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore.Builder#open open()} methods.)
 * This information is critical for maintaining an accurate view of the record store's meta-data version in the
 * face of meta-data updates, but reading this information with every transaction can create hot spots in the
 * underlying database that can degrade performance. To avoid that performance pathology, the classes in
 * this package can be used to cache the store state information.
 */
package com.apple.foundationdb.record.provider.foundationdb.storestate;
