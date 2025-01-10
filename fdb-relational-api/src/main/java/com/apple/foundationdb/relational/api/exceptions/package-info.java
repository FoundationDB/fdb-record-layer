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
 * Exceptions thrown by the Relational packages. These should generally be wrapped by a
 * {@link java.sql.SQLException} so that JDBC consumers don't need to know about our exception types.
 *
 * @see com.apple.foundationdb.relational.api.exceptions.ContextualSQLException
 */
package com.apple.foundationdb.relational.api.exceptions;
