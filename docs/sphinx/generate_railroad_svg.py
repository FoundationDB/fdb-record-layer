#!/bin/python
#
# generate_railroad_svg.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

from os import walk, path
from railroad import Diagram, Choice, Terminal, NonTerminal, OneOrMore, Sequence, Optional, Stack, Group
import railroad

# Vertical space
railroad.VS = 15

# Arc radius
railroad.AR = 12

# Find all diagram files and generate svgs
base_dir = path.dirname(path.abspath(__file__))
for root, dirs, files in walk('.'):
    for file in files:
        if file.endswith('.diagram'):
            with open(path.join(root, file), 'r') as diagram_file:
                diagram = eval(diagram_file.read())
                with open(path.join(root, file + ".svg"), 'w') as svg_file:
                    diagram.writeStandalone(svg_file.write)

