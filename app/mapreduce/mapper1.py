#!/usr/bin/env python3
"""Pipeline 1 Mapper: Tokenize documents and emit (term, doc_id) pairs.

Input format (from /input/data):  doc_id\ttitle\ttext
Output format:                    term\tdoc_id\t1
"""
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 2)
    if len(parts) < 3:
        continue
    doc_id, title, text = parts
    # Index both title and text
    content = title.replace('_', ' ') + ' ' + text
    # Tokenize: lowercase, extract alphanumeric words of length >= 2
    tokens = re.findall(r'[a-z0-9]{2,}', content.lower())
    for token in tokens:
        print(f'{token}\t{doc_id}\t1')
