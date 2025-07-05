# app/features/tools/dwd_pipeline/utils.py

import re
from datetime import datetime

KNOWN_VARIANTS = {"historical", "recent", "now", "meta_data", "metadata"}

def classify_content(lines, filename=""):
    fname = filename.lower()
    if fname.startswith(("metadaten_", "beschreibung", "stationen")):
        return "metadata"
    if "produkt_" in fname or "stundenwerte_" in fname or "zehn_min_" in fname:
        return "raw"
    joined = " ".join(lines).lower()
    if "mess_datum" in joined and any(p in joined for p in ["tt_", "rf_", "pp_", "fx", "fm", "n", "sd", "sh", "qn", "rs"]):
        return "raw"
    return "unknown"

def extract_dataset_and_variant(source_path):
    if source_path == "unknown":
        return "unknown", "unknown"

    parts = source_path.strip("/").split("/")
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].lower() in KNOWN_VARIANTS:
            dataset = "_".join(parts[:i])
            variant = parts[i].lower()
            return dataset, variant
    dataset = "_".join(parts)
    return dataset, None

def match_by_interval(rfrom, rto, metadata_rows):
    results = []
    try:
        r_start = datetime.strptime(rfrom, "%Y-%m-%d")
        r_end = datetime.strptime(rto, "%Y-%m-%d")
    except Exception:
        return results

    for row in metadata_rows:
        f, t = row.get("from"), row.get("to")
        if not f or not t:
            continue
        try:
            mfrom = datetime.strptime(f, "%Y-%m-%d")
            mto = datetime.strptime(t, "%Y-%m-%d")
            if mfrom <= r_end and mto >= r_start:
                results.append(row)
        except:
            continue
    return results