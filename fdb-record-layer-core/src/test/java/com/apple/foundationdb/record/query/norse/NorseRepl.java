/*
 * NorseRepl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.norse;

import com.apple.foundationdb.record.query.plan.debug.PlannerRepl;

public class NorseRepl extends PlannerRepl {
    private static final String norseBanner =
            "                   ~.                       \n" +
            "            Ya...___|__..ab.     .   .  \n" +
            "             Y88b  \\88b  \\88b   (     )  \n" +
            "              Y88b  :88b  :88b   `.oo'   \n" +
            "              :888  |888  |888  ( (`-'   \n" +
            "     .---.    d88P  ;88P  ;88P   `.`.    \n" +
            "    / .-._)  d8P-\"\"\"|\"\"\"'-Y8P      `.`.  \n" +
            "   ( (`._) .-.  .-. |.-.  .-.  .-.   ) ) \n" +
            "    \\ `---( O )( O )( O )( O )( O )-' /  \n" +
            "     `.    `-'  `-'  `-'  `-'  `-'  .' CJ \n" +
            "       `---------------------------'\n";

    @Override
    public String getBanner() {
        return norseBanner;
    }
}
