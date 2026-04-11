#!/usr/bin/env python3
"""Pipeline 2 Reducer: Compute document frequency (df) per term.

Input format (sorted by key):  term\t1
Output format:                 term\tdf
"""
import sys

current_term = None
current_df = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 2:
        continue
    term, count = parts
    try:
        count = int(count)
    except ValueError:
        continue

    if term == current_term:
        current_df += count
    else:
        if current_term is not None:
            print(f'{current_term}\t{current_df}')
        current_term = term
        current_df = count

if current_term is not None:
    print(f'{current_term}\t{current_df}')
