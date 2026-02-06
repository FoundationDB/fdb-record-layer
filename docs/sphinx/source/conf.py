#
# conf.py
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

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'FoundationDB Record Layer'
copyright = '2025, Apple Inc.'
author = 'Apple Inc.'

extensions = ['myst_parser', 'sphinx_design']

templates_path = ['_templates']
exclude_patterns = []


html_theme = 'furo'
html_theme_options = {
}

html_static_path = ['_static']
html_show_sphinx = False

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

root_doc = 'index'

suppress_warnings = ['misc.highlighting_failure']

rst_prolog = """
.. role:: sql(code)
   :language: sql


.. role:: json(code)
   :language: json


.. role:: java(code)
   :language: java
"""

myst_heading_anchors = 4
myst_enable_extensions = ["colon_fence"]

