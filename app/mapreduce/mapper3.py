#!/usr/bin/env python3
"""Pipeline 3 Mapper: Emit doc_id and tf for document length computation.

Input format (from /indexer/index):  term\tdoc_id\ttf
Output format:                       doc_id\ttf
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 3:
        continue
    _, doc_id, tf = parts
    print(f'{doc_id}\t{tf}')
