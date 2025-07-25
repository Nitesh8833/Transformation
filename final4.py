"""
provider_location_pipeline.py
─────────────────────────────────────────────────────────────────
Builds “Provider_Locations_Final” with:
• header‑scan metadata
• duplicate‑header protection
• synonym‑aware fallback merge (ER ⇔ Emergency‑Room, Surg ⇔ Surgery, OR)
• M/D/YYYY date columns
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd, re, platform


# ───────────────────────── HEADER‑SCAN HELPER ─────────────────────────
def get_header_meta(path: Path | str,
                    sheet: str = "BMH Locations") -> dict[str, str | None]:
    df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)
    hdr_idx = df.eq("Street Address").any(axis=1).idxmax()
    top = df.iloc[:hdr_idx]

    meta = {"CAQH Grp ID": None, "TIN": None, "BILLING": None, "MEDICAID BOX": None}

    def right_of(r, c):
        for cc in range(c + 1, top.shape[1]):
            v = top.iat[r, cc]
            if pd.notna(v) and str(v).strip():
                return str(v).strip()
        return ""

    for r in range(top.shape[0]):
        for c in range(top.shape[1]):
            cell = top.iat[r, c]
            if pd.isna(cell):
                continue
            txt, up = str(cell).strip(), str(cell).strip().upper()

            if up.startswith("CAQH GRP ID") and not meta["CAQH Grp ID"]:
                meta["CAQH Grp ID"] = re.search(r"\d+", txt).group(0) if re.search(r"\d+", txt) else right_of(r, c)
            elif up.startswith("TIN") and not meta["TIN"]:
                m = re.search(r"TIN\s*[:\s]*([0-9\-]+)", txt, re.I)
                meta["TIN"] = m.group(1) if m else right_of(r, c)
            elif up.startswith("BILLING") and not meta["BILLING"]:
                after = txt.split(":", 1)[1].strip() if ":" in txt else ""
                meta["BILLING"] = after or right_of(r, c)
            elif up.startswith("MEDICAID BOX") and not meta["MEDICAID BOX"]:
                after = txt.split(":", 1)[1].strip() if ":" in txt else ""
                meta["MEDICAID BOX"] = after or right_of(r, c)
    return meta


# ────────────────────────── PIPELINE CLASS ──────────────────────────
class ProviderLocationPipeline:
    # ─── init ───
    def __init__(self, input_xlsx: str | Path):
        self.input_xlsx = Path(input_xlsx).expanduser().resolve()
        if not self.input_xlsx.exists():
            raise FileNotFoundError(self.input_xlsx)

        self.output_dir  = self.input_xlsx.parent / "output"
        self.output_dir.mkdir(exist_ok=True)
        self.output_xlsx = self.output_dir / "provider_locations_final5.xlsx"
        self._sheets: dict[str, pd.DataFrame] = {}

    # ─── helpers ───
    @staticmethod
    def _clean(s): return "" if pd.isna(s) else str(s).strip()

    @staticmethod
    def _dedup_columns(cols):
        seen, out = {}, []
        for col in cols:
            k = seen.get(col, 0)
            out.append(col if k == 0 else f"{col}_{k}")
            seen[col] = k + 1
        return out

    def _load(self, sheet: str) -> pd.DataFrame:
        if sheet not in self._sheets:
            self._sheets[sheet] = pd.read_excel(self.input_xlsx, sheet_name=sheet, dtype=str)
        return self._sheets[sheet]

    _WORD_MAP = {
        "surgery": "surg", "surg": "surg",
        "operatingroom": "or", "or": "or",
        "er": "er", "emergencyroom": "er"
    }
    @classmethod
    def _norm_word(cls, w: str) -> str:
        key = re.sub(r"[^a-z0-9]", "", w.lower())
        return cls._WORD_MAP.get(key, key)

    def _fmt_date_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        fmt = '%#m/%#d/%Y' if platform.system() == "Windows" else '%-m/%-d/%Y'
        for col in {"Start Date", "DOB", "License Exp", "DEA Exp"} & set(df.columns):
            df[col] = (pd.to_datetime(df[col], errors='coerce')
                         .dt.strftime(fmt)
                         .where(df[col].notna(), df[col]))
        return df

    # ─── stage 1 – locations ───
    def _extract_location_metadata(self) -> pd.DataFrame:
        loc_raw = self._load("BMH Locations").copy()
        hdr_idx = loc_raw.eq("Street Address").any(axis=1).idxmax()

        loc = loc_raw.iloc[hdr_idx + 1:].copy()
        loc.columns = self._dedup_columns(list(loc_raw.iloc[hdr_idx]))
        loc = loc[loc["Street Address"] != "Street Address"]

        for k, v in get_header_meta(self.input_xlsx).items():
            loc[k] = v
        return loc.reset_index(drop=True)

    # ─── stage 2 – provider rows ───
    def _build_provider_rows(self) -> pd.DataFrame:
        GRID = [
            "BMH Comfort Clinic","BMH Danville Clinic","BMH Family Medical Center",
            "BMH Medical Clinic","BMH Brighter Futures","BMH Specialty Clinic",
        ]
        EXTRA = [
            "Start Date","Degree","Specialty","CAQH","NPI","DOB","SS#",
            "License Number","License Exp","DEA#","DEA Exp",
        ]
        roster = self._load("BMH Provider Roster").copy()
        grid   = self._load("BMH Provider Location Grid").copy()

        for df in (roster, grid):
            df["Last Name"]  = df["Last Name"].map(self._clean)
            df["First Name"] = df["First Name"].map(self._clean)
        grid["Middle Name"] = grid["Middle Name"].map(self._clean)

        aff_df = (grid[["Last Name","First Name","BMH Phys Grp"]]
            .assign(Hospital_Affiliation=lambda d: d["BMH Phys Grp"]
                    .map(self._clean).str.lower().eq("affiliated")
                    .map({True:"BMH Phys Grp", False:""}))
            [["Last Name","First Name","Hospital_Affiliation"]])

        merged = pd.merge(
            roster[["Last Name","First Name","Primary Location","Primary Location Address"] + EXTRA],
            grid  [["Last Name","First Name","Middle Name"] + GRID],
            on=["Last Name","First Name"], how="left",
        )

        rows = []
        for _, r in merged.iterrows():
            base = {"Last Name": r["Last Name"], "First Name": r["First Name"],
                    "Middle Name": r.get("Middle Name",""),
                    "Primary Location Address": r.get("Primary Location Address","")}
            base.update({c: r.get(c,"") for c in EXTRA})

            rows.append(base | {"Location": r.get("Primary Location",""),
                                "PRIMARY PRACTICE LOCATION Y/N":"Y"})
            for col in GRID:
                if self._clean(r.get(col)).lower() == "covering":
                    rows.append(base | {"Location": col,
                                        "PRIMARY PRACTICE LOCATION Y/N":"N"})

        df = (pd.DataFrame(rows)
              .merge(aff_df,on=["Last Name","First Name"],how="left")
              .rename(columns={"Hospital_Affiliation":"Hospital Affiliation"}))

        front = ["Last Name","First Name","Middle Name",
                 "Location","PRIMARY PRACTICE LOCATION Y/N","Primary Location Address"]
        return df[front + [c for c in df.columns if c not in front]]

    # ─── stage 3 – merge & polish ───
    def _build_final(self, prov: pd.DataFrame, loc: pd.DataFrame) -> pd.DataFrame:
        final = prov.merge(
            loc, left_on="Location", right_on="LOC DBA NAME",
            how="left", suffixes=("", "_loc")
        ).drop(columns=["LOC DBA NAME"])

        # fallback for blanks
        addr_cols = [c for c in loc.columns if c not in prov.columns]
        fp = lambda s: re.sub(r"[^a-z0-9]", "", str(s).lower())[:10]

        for idx in final.index[final["Street Address"].isna()]:
            loc_frag = final.at[idx, "Location"].lower().strip()
            addr_fp  = fp(final.at[idx, "Primary Location Address"])

            cand = loc[loc["Street Address"].str.lower()
                       .str.replace(r"[^a-z0-9]", "", regex=True)
                       .str.startswith(addr_fp, na=False)]

            if len(cand) > 1:
                words = {self._norm_word(w) for w in re.split(r"\W+", loc_frag) if w}
                def keeper(dba):
                    dba_words = {self._norm_word(w) for w in re.split(r"\W+", dba)}
                    return words.issubset(dba_words)
                cand = cand[cand["LOC DBA NAME"].apply(lambda s: keeper(str(s)) if pd.notna(s) else False)]

            if cand.empty:
                cand = loc[loc["LOC DBA NAME"].str.lower()
                           .str.contains(loc_frag, na=False)].head(1)

            if len(cand) == 1:
                best = cand.iloc[0]
                for col in addr_cols:
                    final.at[idx, col] = best[col]

        final.columns = self._dedup_columns(list(final.columns))
        final = self._fmt_date_cols(final)

        if "Hospital Affiliation" in final.columns:
            final = final[[c for c in final.columns if c != "Hospital Affiliation"]
                          + ["Hospital Affiliation"]]
        return final

    # ─── API ───
    def run(self) -> pd.DataFrame:
        loc  = self._extract_location_metadata()
        prov = self._build_provider_rows()
        self.final_df = self._build_final(prov, loc)
        return self.final_df

    def export(self) -> Path:
        if not hasattr(self, "final_df"):
            raise RuntimeError("Call run() first")
        with pd.ExcelWriter(self.output_xlsx, engine="openpyxl") as xls:
            self.final_df.to_excel(xls, sheet_name="Provider_Locations_Final", index=False)
        print(f"✓ Exported → {self.output_xlsx}")
        return self.output_xlsx


# ─── script entry ───
if __name__ == "__main__":
    PIPE = ProviderLocationPipeline(r"D:\Python_Task\Data2\input.xlsx")  # ← change path
    PIPE.run()
    PIPE.export()
