/*
 * AtomKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;

/**
 * An "atom expression" is one that has semantic meaning; that is, its presence is significant to the meaning of
 * the expression. A non-atom expression (such as Then) is just a holder for its children; in particular, multiple
 * nested non-atom expressions could be collapsed into a single one with the same semantics.
 */
@API(API.Status.EXPERIMENTAL)
public interface AtomKeyExpression extends KeyExpression {
    boolean equalsAtomic(AtomKeyExpression other);
}
