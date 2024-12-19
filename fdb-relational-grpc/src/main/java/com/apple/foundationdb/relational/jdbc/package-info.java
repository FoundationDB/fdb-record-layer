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
 * Utility shared by client and server.
 * Facades over raw protobuf types to present Relational-customization views of base JDBC types.
 * Protobuf schema is such that it facilitates type convertions to make it easy to convert a
 * List of {@link com.apple.foundationdb.relational.api.RelationalStruct} into a {@link com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet}
 * and so on.
 */
package com.apple.foundationdb.relational.jdbc;
