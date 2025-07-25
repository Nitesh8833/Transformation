"""
provider_location_pipeline.py
─────────────────────────────────────────────────────────────────
• Builds one sheet “Provider_Locations_Final”.
• Header‑scan helper pulls CAQH Grp ID / TIN / BILLING / MEDICAID BOX.
• Date helper formats Start Date, DOB, License Exp, DEA Exp as M/D/YYYY.
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import re
import platform


# ───────────────────────────────────────────────────────────────
# 1.  HEADER‑SCAN HELPER  (unchanged)
# ───────────────────────────────────────────────────────────────
def get_header_meta(path: Path | str,
                    sheet: str = "BMH Locations") -> dict[str, str | None]:
    df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)
    hdr_idx = df.eq("Street Address").any(axis=1).idxmax()
    top = df.iloc[:hdr_idx]

    meta = {"CAQH Grp ID": None, "TIN": None, "BILLING": None, "MEDICAID BOX": None}

    def scan_right(r, c):
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
            txt = str(cell).strip()
            up  = txt.upper()

            if up.startswith("CAQH GRP ID") and not meta["CAQH Grp ID"]:
                m = re.search(r"(\d+)", txt)
                meta["CAQH Grp ID"] = m.group(1) if m else scan_right(r, c)

            elif up.startswith("TIN") and not meta["TIN"]:
                m = re.search(r"TIN\s*[:\s]*([0-9\-]+)", txt, re.I)
                meta["TIN"] = m.group(1) if m else scan_right(r, c)

            elif up.startswith("BILLING") and not meta["BILLING"]:
                after = txt.split(":", 1)[1].strip() if ":" in txt else ""
                meta["BILLING"] = after or scan_right(r, c)

            elif up.startswith("MEDICAID BOX") and not meta["MEDICAID BOX"]:
                after = txt.split(":", 1)[1].strip() if ":" in txt else ""
                meta["MEDICAID BOX"] = after or scan_right(r, c)
    return meta


# ──────────────────── MAIN PIPELINE CLASS ────────────────────
class ProviderLocationPipeline:
    """End‑to‑end builder for “Provider_Locations_Final”."""

    # ---------- init ----------
    def __init__(self, input_xlsx: str | Path):
        self.input_xlsx = Path(input_xlsx).expanduser().resolve()
        if not self.input_xlsx.exists():
            raise FileNotFoundError(self.input_xlsx)

        self.output_dir  = self.input_xlsx.parent / "output"
        self.output_dir.mkdir(exist_ok=True)
        self.output_xlsx = self.output_dir / "provider_locations_final3.xlsx"
        self._sheets: dict[str, pd.DataFrame] = {}

    # ---------- small helpers ----------
    @staticmethod
    def _clean(s): return "" if pd.isna(s) else str(s).strip()

    def _load(self, sheet: str) -> pd.DataFrame:
        if sheet not in self._sheets:
            self._sheets[sheet] = pd.read_excel(self.input_xlsx, sheet_name=sheet, dtype=str)
        return self._sheets[sheet]

    # 🟢 ---------- date‑format helper ----------
    def _fmt_date_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format Start Date, DOB, License Exp, DEA Exp → M/D/YYYY."""
        fmt = '%#m/%#d/%Y' if platform.system() == "Windows" else '%-m/%-d/%Y'
        DATE_COLS = {"Start Date", "DOB", "License Exp", "DEA Exp"}

        for col in DATE_COLS & set(df.columns):
            df[col] = (pd.to_datetime(df[col], errors='coerce')
                         .dt.strftime(fmt)
                         .where(df[col].notna(), df[col]))
        return df

    # ---------- LOCATION METADATA ----------
    def _extract_location_metadata(self) -> pd.DataFrame:
        loc_raw = self._load("BMH Locations").copy()
        hdr_idx = loc_raw.eq("Street Address").any(axis=1).idxmax()
        loc = loc_raw.iloc[hdr_idx + 1:].copy()
        loc.columns = loc_raw.iloc[hdr_idx]
        loc = loc[loc["Street Address"] != "Street Address"]

        meta = get_header_meta(self.input_xlsx)
        for k, v in meta.items(): loc[k] = v
        return loc.reset_index(drop=True)

    # ---------- PROVIDER ROWS ----------
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
            df["Last Name"] = df["Last Name"].map(self._clean)
            df["First Name"] = df["First Name"].map(self._clean)
        grid["Middle Name"] = grid["Middle Name"].map(self._clean)

        aff_df = (
            grid[["Last Name","First Name","BMH Phys Grp"]]
            .assign(Hospital_Affiliation=lambda d: d["BMH Phys Grp"]
                    .map(self._clean).str.lower().eq("affiliated")
                    .map({True:"BMH Phys Grp", False:""}))
            [["Last Name","First Name","Hospital_Affiliation"]]
        )

        merged = pd.merge(
            roster[["Last Name","First Name","Primary Location"] + EXTRA],
            grid  [["Last Name","First Name","Middle Name"] + GRID],
            on=["Last Name","First Name"], how="left",
        )

        rows = []
        for _, r in merged.iterrows():
            base = {"Last Name":r["Last Name"], "First Name":r["First Name"],
                    "Middle Name":r.get("Middle Name","")}
            base.update({col:r.get(col,"") for col in EXTRA})

            rows.append(base | {"Location":r.get("Primary Location",""),
                                "PRIMARY PRACTICE LOCATION Y/N":"Y"})
            for col in GRID:
                if self._clean(r.get(col)).lower() == "covering":
                    rows.append(base | {"Location":col,
                                        "PRIMARY PRACTICE LOCATION Y/N":"N"})

        df = (pd.DataFrame(rows)
              .merge(aff_df,on=["Last Name","First Name"],how="left")
              .rename(columns={"Hospital_Affiliation":"Hospital Affiliation"}))

        front = ["Last Name","First Name","Middle Name","Location","PRIMARY PRACTICE LOCATION Y/N"]
        return df[front + [c for c in df.columns if c not in front]]

    # ---------- MERGE & EXPORT ----------
    def _build_final(self, prov: pd.DataFrame, loc: pd.DataFrame) -> pd.DataFrame:
        final = prov.merge(
            loc, left_on="Location", right_on="LOC DBA NAME",
            how="left", suffixes=("", "_loc")
        ).drop(columns=["LOC DBA NAME"])

        final = self._fmt_date_cols(final)       # 🟢 format the 4 date columns

        if "Hospital Affiliation" in final.columns:
            final = final[[c for c in final.columns if c != "Hospital Affiliation"]
                          + ["Hospital Affiliation"]]
        return final

    # ---------- public API ----------
    def run(self) -> pd.DataFrame:
        prov = self._build_provider_rows()
        loc  = self._extract_location_metadata()
        self.final_df = self._build_final(prov, loc)
        return self.final_df

    def export(self) -> Path:
        if not hasattr(self, "final_df"):
            raise RuntimeError("Call run() first")
        with pd.ExcelWriter(self.output_xlsx, engine="openpyxl") as xls:
            self.final_df.to_excel(xls, sheet_name="Provider_Locations_Final", index=False)
        print(f"✓ Exported → {self.output_xlsx}")
        return self.output_xlsx


# ---------------- script entry ----------------------
if __name__ == "__main__":
    pipe = ProviderLocationPipeline(r"D:\Python_Task\Data2\input.xlsx")  # ← change path
    pipe.run()
    pipe.export()
