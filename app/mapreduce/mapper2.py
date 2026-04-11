#!/usr/bin/env python3
"""Pipeline 2 Mapper: Emit term counts for document frequency computation.

Input format (from /indexer/index):  term\tdoc_id\ttf
Output format:                       term\t1
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 3:
        continue
    term = parts[0]
    # Each (term, doc_id) entry represents one document containing the term
    print(f'{term}\t1')
