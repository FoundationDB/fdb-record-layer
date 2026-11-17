#!/usr/bin/env python3
#
# fix-unused-imports.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Remove the imports that checkstyle reports as unused.

Used by the /auto-qa skill after it deletes tests from a companion *AutoQATest.java file.
Only files whose name ends in AutoQATest.java are changed.

Usage: auto-qa/fix-unused-imports.py <gradle checkstyle output file>
"""

import collections
import re
import sys

PATTERN = re.compile(r"(/\S+AutoQATest\.java):\d+:\d+: Unused import - (\S+)\. \[UnusedImports\]")


def main(log_path):
    todo = collections.defaultdict(set)
    with open(log_path) as log:
        for path, imported in PATTERN.findall(log.read()):
            todo[path].add(imported)
    for path, imports in sorted(todo.items()):
        targets = {"import " + i + ";" for i in imports} | {"import static " + i + ";" for i in imports}
        with open(path) as f:
            lines = f.read().split("\n")
        kept = [line for line in lines if line not in targets]
        text = "\n".join(kept)
        while "\n\n\n" in text:
            text = text.replace("\n\n\n", "\n\n")
        with open(path, "w") as f:
            f.write(text)
        print("%s: removed %d unused import(s)" % (path, len(lines) - len(kept)))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
