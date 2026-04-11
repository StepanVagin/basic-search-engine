#!/usr/bin/env python3
"""Pipeline 3 Reducer: Compute document length (total tokens) per document.

Input format (sorted by key):  doc_id\ttf
Output format:                 doc_id\tdoc_length
"""
import sys

current_doc = None
current_length = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 2:
        continue
    doc_id, tf = parts
    try:
        tf = int(tf)
    except ValueError:
        continue

    if doc_id == current_doc:
        current_length += tf
    else:
        if current_doc is not None:
            print(f'{current_doc}\t{current_length}')
        current_doc = doc_id
        current_length = tf

if current_doc is not None:
    print(f'{current_doc}\t{current_length}')
