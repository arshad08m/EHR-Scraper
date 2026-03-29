"""
utils/enrichment.py — joins scraped orders with the master Excel file
(patients + physicians + NPIs) built earlier.

Supports both input formats:
    - JSON array (data/orders_output.json)
    - JSONL      (data/orders_output.jsonl)
"""

import json
import argparse
from pathlib import Path
import pandas as pd
from rich.console import Console

console = Console()


def load_master(excel_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load the master Excel.
    Expected sheets (case-insensitive match):
      - 'Patients'   : patient_id/MRN, client_name (must match order client_name)
      - 'Physicians' : physician_id, npi, physician_name
    Returns (patients_df, physicians_df)
    """
    xl   = pd.ExcelFile(excel_path)
    sheets = {s.lower(): s for s in xl.sheet_names}

    pat_sheet = sheets.get("patients") or xl.sheet_names[0]
    phy_sheet = sheets.get("physicians") or (xl.sheet_names[1] if len(xl.sheet_names) > 1 else xl.sheet_names[0])

    patients   = xl.parse(pat_sheet)
    physicians = xl.parse(phy_sheet)

    # Normalize column names
    patients.columns   = [c.strip().lower().replace(" ", "_") for c in patients.columns]
    physicians.columns = [c.strip().lower().replace(" ", "_") for c in physicians.columns]

    console.log(f"Patients sheet   : {len(patients)} rows, cols: {list(patients.columns)}")
    console.log(f"Physicians sheet : {len(physicians)} rows, cols: {list(physicians.columns)}")
    return patients, physicians


def enrich_jsonl(
    orders_path:   str | Path,
    master_path:   str | Path,
    output_path:   str | Path,
    client_col:    str = "client_name",
    patient_key:   str = "patient_id",
    physician_key: str = "physician_id",
    npi_col:       str = "npi",
):
    """
    Read orders (JSON array or JSONL), join with master Excel,
    write enriched output as JSON (if output suffix is .json) or JSONL.
    Missing joins are flagged rather than dropped.
    """
    orders_path = Path(orders_path)
    output_path = Path(output_path)

    patients, physicians = load_master(master_path)

    # Build lookup dicts  {normalized_name → {patient_id, ...}}
    def _norm(s): return str(s).strip().upper()

    pat_lookup = {
        _norm(row.get(client_col, "")): row.to_dict()
        for _, row in patients.iterrows()
        if row.get(client_col)
    }
    phy_lookup = {
        str(row.get(physician_key, "")).strip(): row.to_dict()
        for _, row in physicians.iterrows()
        if row.get(physician_key)
    }

    records: list[dict] = []
    if orders_path.suffix.lower() == ".json":
        with open(orders_path, encoding="utf-8") as fin:
            raw = json.load(fin)
        if isinstance(raw, list):
            records = [r for r in raw if isinstance(r, dict)]
    else:
        with open(orders_path, encoding="utf-8") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    if isinstance(row, dict):
                        records.append(row)
                except Exception:
                    continue

    enriched_rows: list[dict] = []
    unmatched_pat = unmatched_phy = 0

    for record in records:
        # Join patient
        key = _norm(record.get("client_name", ""))
        pat = pat_lookup.get(key)
        if pat:
            record["patient_id"]  = pat.get(patient_key, "")
            record["patient_dob"] = pat.get("date_of_birth", "")
            record["patient_match"] = True
        else:
            record["patient_id"]    = None
            record["patient_match"] = False
            unmatched_pat += 1

        # Join physician via patient -> physician mapping
        phy_id = str(pat.get(physician_key, "") if pat else "").strip()
        phy    = phy_lookup.get(phy_id)
        if phy:
            record["physician_npi"]   = phy.get(npi_col, "")
            record["physician_name"]  = phy.get("physician_name", "")
            record["physician_match"] = True
        else:
            record["physician_npi"]   = None
            record["physician_match"] = False
            unmatched_phy += 1

        enriched_rows.append(record)

    if output_path.suffix.lower() == ".json":
        with open(output_path, "w", encoding="utf-8") as fout:
            json.dump(enriched_rows, fout, ensure_ascii=False, indent=2)
    else:
        with open(output_path, "w", encoding="utf-8") as fout:
            for row in enriched_rows:
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    enriched_count = len(enriched_rows)

    console.log(f"[green]✓ Enriched {enriched_count} records → {output_path}[/green]")
    console.log(f"  Unmatched patients   : {unmatched_pat}")
    console.log(f"  Unmatched physicians : {unmatched_phy}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders",  required=True, help="Path to orders output (.json or .jsonl)")
    parser.add_argument("--master",  required=True, help="Path to master Excel file")
    parser.add_argument("--output",  default="data/orders_enriched.json")
    args = parser.parse_args()
    enrich_jsonl(args.orders, args.master, args.output)
