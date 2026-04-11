#!/usr/bin/env python3
"""Pipeline 1 Reducer: Sum term frequencies per (term, doc_id) pair.

Input format (sorted by key):  term\tdoc_id\t1
Output format:                 term\tdoc_id\ttf
"""
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 3:
        continue
    term, doc_id, count = parts
    key = f'{term}\t{doc_id}'
    try:
        count = int(count)
    except ValueError:
        continue

    if key == current_key:
        current_count += count
    else:
        if current_key is not None:
            print(f'{current_key}\t{current_count}')
        current_key = key
        current_count = count

if current_key is not None:
    print(f'{current_key}\t{current_count}')
