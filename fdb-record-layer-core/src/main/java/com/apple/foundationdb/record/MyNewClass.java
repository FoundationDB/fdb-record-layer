/*
 * MyNewClass.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

/**
 * A new class.
 */
public class MyNewClass {

    /** The possible cases. */
    public enum MyState {
        AAA,
        BBB,
        CCC
    }

    private final MyState state;

    /**
     * Make a new instance.
     * @param state the instance state
     */
    public MyNewClass(final MyState state) {
        this.state = state;
    }

    /**
     * Get summary state.
     * @return whether in state <code>B</code>
     */
    public boolean isB() {
        switch (state) {
            case AAA:
            case CCC:
                return false;
            case BBB:
                return true;
            // Without this, get Checkstyle: switch without "default" clause.
            default:
                throw new IllegalStateException("not in a known state");
        }
    }
}
