/*
 * Utils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package sqlline;

import java.util.List;
import java.util.Locale;

/**
 * Utility for classes in this package only.
 */
final class Utils {
    private Utils() {
    }

    /**
     * Remove command name from start of the string. Treat the remainder
     * as command argument.
     * @param line Full line given to command; includes command prefix to strip.
     * @param List of command names.
     * @return Command argument.
     */
    static String stripCommandName(String line, List<String> commandNames) {
        String lowerCaseLine = line.toLowerCase(Locale.getDefault());
        String remainder = null;
        for (String name : commandNames) {
            if (lowerCaseLine.startsWith(name.toLowerCase(Locale.getDefault()).trim())) {
                remainder = line.substring(name.length());
                break;
            }
        }
        return remainder == null ? remainder : remainder.trim();
    }
}
