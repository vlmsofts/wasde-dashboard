#!/usr/bin/env python3
"""
wasde_data_builder.py -- VLM Commodities
========================================
Reads wasde_cotton_master.csv and generates the JavaScript data blob
that gets injected into cotton_wasde_dashboard.html.

Run this after wasde_updater.py has downloaded a new WASDE CSV,
OR run manually after adding data to the master CSV.

Usage:
    python wasde_data_builder.py                  # build & inject into dashboard
    python wasde_data_builder.py --json-only      # just write wasde_full_data.json
    python wasde_data_builder.py --check          # print coverage report only

Output:
    wasde_full_data.json           (standalone JSON for debugging)
    cotton_wasde_dashboard.html    (updated with new const W = {...} blob)
"""

import pandas as pd
import json
import re
import os
import sys
import argparse
from datetime import date

# -- Paths ---------------------------------------------------------------------
DIR            = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV     = os.path.join(DIR, 'wasde_cotton_master.csv')
OUTPUT_JSON    = os.path.join(DIR, 'wasde_full_data.json')
DASHBOARD_HTML = os.path.join(DIR, 'cotton_wasde_dashboard.html')

# -- Constants -----------------------------------------------------------------
ATTRS = ['Beginning Stocks', 'Production', 'Imports', 'Domestic Use',
         'Exports', 'Loss', 'Ending Stocks']

REGIONS_PRIMARY = [
    'United States', 'World', 'India', 'China',
    'Brazil', 'Australia', 'Pakistan',
]

REGIONS_EXTENDED = [
    'United States', 'World', 'India', 'China', 'Brazil', 'Australia',
    'Pakistan', 'Total Foreign', 'Major Exporters', 'Major Importers',
    'Central Asia', 'Afr. Fr. Zone', 'S. Hemis.', 'Mexico', 'Turkey',
    'Bangladesh', 'Vietnam', 'European Union', 'Indonesia', 'Thailand',
    'World Less China',
]

MONTH_NAMES = {
    1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun',
    7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec',
}

MONTH_ORDER_SEASONAL = [
    'May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr'
]

WASDE_SCHEDULE = [
    {'num':667, 'date':'2026-01-12', 'label':'Jan 2026', 'newCrop':False},
    {'num':668, 'date':'2026-02-10', 'label':'Feb 2026', 'newCrop':False},
    {'num':669, 'date':'2026-03-10', 'label':'Mar 2026', 'newCrop':False},
    {'num':670, 'date':'2026-04-09', 'label':'Apr 2026', 'newCrop':False},
    {'num':671, 'date':'2026-05-12', 'label':'May 2026', 'newCrop':True},
    {'num':672, 'date':'2026-06-11', 'label':'Jun 2026', 'newCrop':False},
    {'num':673, 'date':'2026-07-10', 'label':'Jul 2026', 'newCrop':False},
    {'num':674, 'date':'2026-08-12', 'label':'Aug 2026', 'newCrop':False},
    {'num':675, 'date':'2026-09-11', 'label':'Sep 2026', 'newCrop':False},
    {'num':676, 'date':'2026-10-09', 'label':'Oct 2026', 'newCrop':False},
    {'num':677, 'date':'2026-11-10', 'label':'Nov 2026', 'newCrop':False},
    {'num':678, 'date':'2026-12-10', 'label':'Dec 2026', 'newCrop':False},
]

# Region name normalization (applied on load)
REGION_MAP = {
    'S. Hemis':        'S. Hemis.',
    'Major exporters': 'Major Exporters',
    'Major importers': 'Major Importers',
    'Total foreign':   'Total Foreign',
    'EU-27+UK':        'EU-27',
}

# -- Load & normalize -----------------------------------------------------------
def load_master():
    if not os.path.exists(MASTER_CSV):
        print(f"ERROR: {MASTER_CSV} not found.")
        print("Run wasde_updater.py first, or place the master CSV in this directory.")
        sys.exit(1)

    df = pd.read_csv(MASTER_CSV)
    df['region'] = df['region'].map(lambda r: REGION_MAP.get(r, r) if pd.notna(r) else r)
    df['value']  = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value', 'region', 'market_year', 'attribute'])
    df['wasde_num']      = df['wasde_num'].astype(int)
    df['forecast_year']  = df['forecast_year'].astype(int)
    df['forecast_month'] = df['forecast_month'].astype(int)
    return df

# -- Core build functions -------------------------------------------------------
def build_snapshots(df, regions, all_mys, latest_wasde):
    """Latest and previous-WASDE snapshots for each region/MY/attr."""
    latest = {}
    prev   = {}

    for region in regions:
        latest[region] = {}
        prev[region]   = {}
        rdf = df[df['region'] == region]

        for my in all_mys:
            latest[region][my] = {}
            prev[region][my]   = {}
            mydf = rdf[rdf['market_year'] == my]

            for attr in ATTRS:
                adf = mydf[mydf['attribute'] == attr].sort_values('wasde_num')
                if len(adf):
                    latest[region][my][attr] = round(float(adf.iloc[-1]['value']), 2)
                    prev_df = adf[adf['wasde_num'] < latest_wasde]
                    prev[region][my][attr] = (
                        round(float(prev_df.iloc[-1]['value']), 2) if len(prev_df) else None
                    )
                else:
                    latest[region][my][attr] = None
                    prev[region][my][attr]   = None

    return latest, prev


def build_revisions(df, regions, recent_mys):
    """Month-by-month revision history for each region/MY/attr."""
    revisions = {}

    for region in regions:
        revisions[region] = {}
        rdf = df[df['region'] == region]

        for my in recent_mys:
            revisions[region][my] = {}
            mydf = rdf[rdf['market_year'] == my]

            for attr in ATTRS:
                adf = mydf[mydf['attribute'] == attr].sort_values('wasde_num')
                pts = []
                for _, row in adf.iterrows():
                    mn  = MONTH_NAMES.get(int(row['forecast_month']), '?')
                    yr  = str(int(row['forecast_year']))[-2:]
                    pts.append([f"{mn}'{yr}", round(float(row['value']), 2)])
                revisions[region][my][attr] = pts

    return revisions


def build_history(df, regions, all_mys, latest_snap):
    """All-years trend: latest estimate per marketing year."""
    hist = {}
    for region in regions:
        hist[region] = {}
        for attr in ATTRS:
            pts = [
                [my, latest_snap[region][my][attr]]
                for my in all_mys
                if latest_snap[region].get(my, {}).get(attr) is not None
            ]
            hist[region][attr] = pts
    return hist


def build_seasonal_avg(revisions, all_mys):
    """
    Average monthly revision from the May first projection for US Production.
    Positive = USDA revised UP vs May; Negative = revised DOWN.
    """
    buckets = {m: [] for m in MONTH_ORDER_SEASONAL}

    for my in all_mys[:-1]:   # skip current (incomplete) year
        rev = revisions.get('United States', {}).get(my, {}).get('Production', [])
        if not rev:
            continue
        may_v = next((v for lbl, v in rev if lbl.startswith("May'")), None)
        if may_v is None:
            continue
        for lbl, v in rev:
            mon = lbl[:3]
            if mon in buckets:
                buckets[mon].append(round(v - may_v, 2))

    return {
        m: (round(sum(vals) / len(vals), 2) if vals else 0)
        for m, vals in buckets.items()
    }


def build_may_vs_final(df, regions, all_mys):
    """
    First May projection vs. final estimate for each region/MY/attr.
    Used by the revision history tab's summary table.
    """
    result = {}
    for region in regions:
        result[region] = {}
        rdf = df[df['region'] == region]
        for my in all_mys:
            result[region][my] = {}
            my_start_yr = int(my.split('/')[0])
            mydf = rdf[rdf['market_year'] == my]
            for attr in ATTRS:
                adf = mydf[mydf['attribute'] == attr].sort_values('wasde_num')
                if len(adf) == 0:
                    result[region][my][attr] = {'first_may': None, 'final': None}
                    continue
                may_rows = adf[(adf['forecast_month'] == 5) & (adf['forecast_year'] == my_start_yr)]
                first_may = round(float(may_rows.iloc[0]['value']), 2) if len(may_rows) else None
                final_val = round(float(adf.iloc[-1]['value']), 2)
                result[region][my][attr] = {'first_may': first_may, 'final': final_val}
    return result


# -- Inject into dashboard ------------------------------------------------------
def inject_into_dashboard(js_blob):
    if not os.path.exists(DASHBOARD_HTML):
        print(f"  WARN: Dashboard not found at {DASHBOARD_HTML} -- skipping injection.")
        return False

    with open(DASHBOARD_HTML, 'r', encoding='utf-8') as f:
        html = f.read()

    new_blob = 'const W = ' + js_blob + ';'
    pattern  = r'const W = \{.*?\};'

    if not re.search(pattern, html, re.DOTALL):
        print("  WARN: Could not find 'const W = {...}' marker in dashboard HTML.")
        return False

    new_html = re.sub(pattern, new_blob, html, count=1, flags=re.DOTALL)

    with open(DASHBOARD_HTML, 'w', encoding='utf-8') as f:
        f.write(new_html)

    print(f"  [OK] Dashboard injected: {DASHBOARD_HTML}")
    return True


# -- Coverage report ------------------------------------------------------------
def print_coverage(df):
    print("\n=== DATA COVERAGE REPORT ===")
    all_mys = sorted(df['market_year'].unique())
    latest  = int(df['wasde_num'].max())
    print(f"Master CSV: {len(df):,} rows | WASDE {df['wasde_num'].min()}-{latest}")
    print(f"{'Market Year':<12} {'WASDEs':>8} {'Range':>16} {'Regions':>8}")
    print("-" * 50)
    for my in all_mys:
        sub    = df[df['market_year'] == my]
        wasdes = sorted(sub['wasde_num'].unique())
        n      = len(wasdes)
        rng    = f"{wasdes[0]}-{wasdes[-1]}" if n else "--"
        print(f"{my:<12} {n:>8} {rng:>16} {sub['region'].nunique():>8}")


# -- Main -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='VLM WASDE Data Builder')
    parser.add_argument('--json-only',  action='store_true', help='Write JSON only, skip dashboard injection')
    parser.add_argument('--check',      action='store_true', help='Print coverage report and exit')
    parser.add_argument('--recent-mys', type=int, default=8,  help='How many recent market years to include in revision history (default: 8)')
    args = parser.parse_args()

    print("VLM Cotton WASDE Data Builder")
    print("=" * 50)

    # Load
    print("[1/5] Loading master CSV...")
    df = load_master()
    print(f"      {len(df):,} rows | WASDE {df['wasde_num'].min()}-{df['wasde_num'].max()}")

    if args.check:
        print_coverage(df)
        return

    all_mys       = sorted(df['market_year'].unique())
    recent_mys    = all_mys[-args.recent_mys:]
    latest_wasde  = int(df['wasde_num'].max())
    prev_wasde    = int(df[df['wasde_num'] < latest_wasde]['wasde_num'].max()) if latest_wasde > df['wasde_num'].min() else latest_wasde

    # Determine labels
    sched_map      = {s['num']: s for s in WASDE_SCHEDULE}
    latest_info    = sched_map.get(latest_wasde, {})
    prev_info      = sched_map.get(prev_wasde, {})
    latest_label   = f"{latest_info.get('label','?')} (WASDE {latest_wasde})"
    prev_label     = f"{prev_info.get('label','?')} (WASDE {prev_wasde})"

    # Build
    print("[2/5] Building snapshots (latest + prev)...")
    latest_snap, prev_snap = build_snapshots(df, REGIONS_EXTENDED, all_mys, latest_wasde)

    print("[3/5] Building revision histories...")
    revisions = build_revisions(df, REGIONS_PRIMARY, recent_mys)

    print("[4/5] Building history & seasonal analysis...")
    hist         = build_history(df, REGIONS_EXTENDED, all_mys, latest_snap)
    seasonal_avg = build_seasonal_avg(revisions, all_mys)
    may_vs_final = build_may_vs_final(df, REGIONS_PRIMARY, all_mys)

    # Assemble
    output = {
        'latest':         latest_snap,
        'prev':           prev_snap,
        'rev':            revisions,
        'hist':           hist,
        'seasonal_avg':   seasonal_avg,
        'may_vs_final':   may_vs_final,
        'regions':        REGIONS_PRIMARY,
        'regions_all':    REGIONS_EXTENDED,
        'attrs':          ATTRS,
        'mys':            all_mys,
        'latest_wasde':   latest_wasde,
        'prev_wasde':     prev_wasde,
        'latest_label':   latest_label,
        'prev_label':     prev_label,
        'wasde_schedule': WASDE_SCHEDULE,
        'built_at':       str(date.today()),
    }

    js_blob = json.dumps(output, separators=(',', ':'))
    print(f"      Data blob: {len(js_blob):,} bytes ({len(js_blob)//1024}KB)")

    # Save JSON
    with open(OUTPUT_JSON, 'w') as f:
        f.write(js_blob)
    print(f"      [OK] Saved: {OUTPUT_JSON}")

    # Inject into dashboard
    print("[5/5] Injecting into dashboard...")
    if not args.json_only:
        inject_into_dashboard(js_blob)
    else:
        print("      Skipped (--json-only)")

    print("\n[OK] Done.")
    print(f"  Latest WASDE: {latest_label}")
    print(f"  Prev   WASDE: {prev_label}")

    # Print quick sanity
    us = latest_snap.get('United States', {}).get('2025/26', {})
    wd = latest_snap.get('World', {}).get('2025/26', {})
    if us:
        print(f"\n  US 2025/26:    Prod={us.get('Production')}  EndStk={us.get('Ending Stocks')}")
    if wd:
        print(f"  World 2025/26: Prod={wd.get('Production')}  EndStk={wd.get('Ending Stocks')}")


if __name__ == '__main__':
    main()
