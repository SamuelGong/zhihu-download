#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
extract_zhihu_batch_urls.py

Batch-extract Zhihu URLs from HTML files under pages/<category>/*.html,
applying in one loop per file both:
  1. <meta itemprop="url"> immediately followed by
     <meta itemprop="datePublished"> OR <meta itemprop="dateModified">
  2. <a target="_blank" data-za-detail-view-element_name="Title">
Normalize protocol-relative URLs (//…) to https://…, de-duplicate per category
(preserving order), and write to urls/<category>.txt (one URL per line).
"""

import os
import sys
from bs4 import BeautifulSoup, NavigableString, Comment

def normalize(raw: str) -> str:
    raw = raw.strip()
    return 'https:' + raw if raw.startswith('//') else raw

def extract_urls_from_soup(soup, seen):
    out = []
    for tag in soup.find_all(True):
        # Case 1: <meta itemprop="url"> + next meta@datePublished
        if tag.name == 'meta' and tag.get('itemprop') == 'url':
            raw = tag.get('content', '').strip()
            if raw:
                sib = tag.next_sibling
                while isinstance(sib, (NavigableString, Comment)):
                    sib = sib.next_sibling
                if (
                        getattr(sib, 'name', None) == 'meta' and
                        sib.get('itemprop') in ('datePublished', 'dateModified')
                ):
                    url = normalize(raw)
                    if url not in seen:
                        seen.add(url)
                        out.append(url)

        # Case 2: <a target="_blank" data-za-detail-view-element_name="Title">
        elif tag.name == 'a' \
             and tag.get('target') == '_blank' \
             and tag.get('data-za-detail-view-element_name') == 'Title':
            raw = tag.get('href', '').strip()
            if raw:
                url = normalize(raw)
                if url not in seen:
                    seen.add(url)
                    out.append(url)
    return out

def main(input_dir='zhihu/pages', output_dir='zhihu/urls'):
    if not os.path.isdir(input_dir):
        sys.exit(f"Error: input directory '{input_dir}' not found. Create it and put your category subfolders inside.")

    os.makedirs(output_dir, exist_ok=True)

    for category in sorted(os.listdir(input_dir)):
        cat_path = os.path.join(input_dir, category)
        if not os.path.isdir(cat_path):
            continue

        seen = set()
        merged = []

        # process each .html in sorted order
        for fn in sorted(f for f in os.listdir(cat_path) if f.lower().endswith('.html')):
            full = os.path.join(cat_path, fn)
            with open(full, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
            result = extract_urls_from_soup(soup, seen)
            merged += result
            print(f'Processed {len(result)} pages for {fn}')

        out_file = os.path.join(output_dir, f"{category}.txt")
        with open(out_file, 'w', encoding='utf-8') as out:
            for url in merged:
                out.write(url + "\n")

        print(f"[{category}] → {len(merged)} URLs → {out_file}")

if __name__ == '__main__':
    main()
