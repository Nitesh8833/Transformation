# provider_location_pipeline.py
# ---------------------------------------------------------------------
from __future__ import annotations

from pathlib import Path
import pandas as pd
import re


class ProviderLocationPipeline:
    """End‑to‑end builder for “Provider_Locations_Final”."""

    # ──────────────────────────── INIT ────────────────────────────────
    def __init__(self, input_xlsx: str | Path):
        self.input_xlsx = Path(input_xlsx).expanduser().resolve()
        if not self.input_xlsx.exists():
            raise FileNotFoundError(self.input_xlsx)

        # output/<same‑file‑name>_final.xlsx
        self.output_dir  = self.input_xlsx.parent / "output"
        self.output_dir.mkdir(exist_ok=True)
        self.output_xlsx = self.output_dir / "provider_locations_final.xlsx"

        # sheet cache
        self._sheets: dict[str, pd.DataFrame] = {}

    # ───────────────────────── HELPERS ────────────────────────────────
    @staticmethod
    def _clean(s) -> str:
        return "" if pd.isna(s) else str(s).strip()

    # ───────────────────────── DATA LOADER ────────────────────────────
    def _load_raw_sheets(self, name: str) -> pd.DataFrame:
        if name not in self._sheets:
            self._sheets[name] = pd.read_excel(self.input_xlsx, sheet_name=name, dtype=str)
        return self._sheets[name]

    # ──────────────────── LOCATION‑METADATA STEP ─────────────────────
    def _extract_location_metadata(self) -> pd.DataFrame:
        loc_raw = self._load_raw_sheets("BMH Locations").copy()

        # header row detection
        hdr_idx = loc_raw.eq("Street Address").any(axis=1).idxmax()
        loc = loc_raw.iloc[hdr_idx + 1 :].copy()
        loc.columns = loc_raw.iloc[hdr_idx]
        loc = loc[loc["Street Address"] != "Street Address"]  # drop dup header rows

        # pull four org‑level fields from the rows above the header
        meta_vals = {}
        for cell in loc_raw.iloc[:hdr_idx].values.flatten():
            if pd.isna(cell):
                continue
            txt = str(cell).strip()
            if txt.startswith("CAQH Grp ID"):
                meta_vals["CAQH Grp ID"] = txt.split("CAQH Grp ID")[-1].strip()
            elif txt.upper().startswith("TIN:"):
                meta_vals["TIN"] = txt.split(":", 1)[1].strip()
            elif txt.upper().startswith("BILLING:"):
                meta_vals["BILLING"] = txt.split(":", 1)[1].strip()
            elif txt.upper().startswith("MEDICAID BOX"):
                meta_vals["MEDICAID BOX"] = txt.split(":", 1)[1].strip()

        for k in ("CAQH Grp ID", "TIN", "BILLING", "MEDICAID BOX"):
            loc[k] = meta_vals.get(k)

        return loc.reset_index(drop=True)

    # ───────────────────── PROVIDER‑ROW STEP ─────────────────────────
    def _build_provider_rows(self) -> pd.DataFrame:
        GRID_COLS = [
            "BMH Comfort Clinic", "BMH Danville Clinic", "BMH Family Medical Center",
            "BMH Medical Clinic", "BMH Brighter Futures", "BMH Specialty Clinic",
        ]
        ROSTER_EXTRA = [
            "Start Date","Degree","Specialty","CAQH","NPI","DOB","ss#",
            "License Number","License Exp","DEA#","DEA Exp",
        ]

        roster = self._load_raw_sheets("BMH Provider Roster").copy()
        grid   = self._load_raw_sheets("BMH Provider Location Grid").copy()

        # clean key fields
        for df in (roster, grid):
            df["Last Name"]  = df["Last Name"].map(self._clean)
            df["First Name"] = df["First Name"].map(self._clean)
        grid["Middle Name"] = grid["Middle Name"].map(self._clean)

        # hospital affiliation
        aff_df = (
            grid[["Last Name","First Name","BMH Phys Grp"]]
            .assign(Hospital_Affiliation=lambda d: d["BMH Phys Grp"]
                    .map(self._clean).str.lower().eq("affiliated")
                    .map({True:"BMH Phys Grp",False:""}))
            [["Last Name","First Name","Hospital_Affiliation"]]
        )

        merged = pd.merge(
            roster[["Last Name","First Name","Primary Location"] + ROSTER_EXTRA],
            grid  [["Last Name","First Name","Middle Name"] + GRID_COLS],
            on=["Last Name","First Name"], how="left",
        )

        # expand primary + covering rows
        rows: list[dict] = []
        for _, r in merged.iterrows():
            base = {
                "Last Name":  r["Last Name"],
                "First Name": r["First Name"],
                "Middle Name":r.get("Middle Name",""),
            }
            base.update({col: r.get(col,"") for col in ROSTER_EXTRA})

            # primary
            rows.append(base | {"Location": r.get("Primary Location",""),
                                "PRIMARY PRACTICE LOCATION Y/N":"Y"})

            # covering
            for col in GRID_COLS:
                if self._clean(r.get(col)).lower() == "covering":
                    rows.append(base | {"Location": col,
                                        "PRIMARY PRACTICE LOCATION Y/N":"N"})

        provider_df = (
            pd.DataFrame(rows)
            .merge(aff_df, on=["Last Name","First Name"], how="left")
            .rename(columns={"Hospital_Affiliation":"Hospital Affiliation"})
        )

        # front columns
        front = ["Last Name","First Name","Middle Name","Location",
                 "PRIMARY PRACTICE LOCATION Y/N"]
        provider_df = provider_df[front
            + [c for c in provider_df.columns if c not in front]]

        return provider_df

    # ───────────────────────── MERGE STEP ───────────────────────────
    def _build_final(
        self,
        provider_df: pd.DataFrame,
        loc_df: pd.DataFrame
    ) -> pd.DataFrame:
        join_left, join_right = "Location", "LOC DBA NAME"

        final = provider_df.merge(
            loc_df,
            left_on=join_left,
            right_on=join_right,
            how="left",
            suffixes=("", "_loc"),
        ).drop(columns=[join_right])

        # move Hospital Affiliation to the end
        last = "Hospital Affiliation"
        if last in final.columns:
            final = final[[c for c in final.columns if c != last] + [last]]

        return final

    # ────────────────────────── PUBLIC API ──────────────────────────
    def run(self) -> pd.DataFrame:
        """Run the full pipeline and return the merged DataFrame."""
        provider_df = self._build_provider_rows()
        loc_df      = self._extract_location_metadata()
        self.final_df = self._build_final(provider_df, loc_df)
        return self.final_df

    def export(self) -> Path:
        """Write only the sheet 'Provider_Locations_Final'."""
        if not hasattr(self, "final_df"):
            raise RuntimeError("Call run() before export().")

        with pd.ExcelWriter(self.output_xlsx, engine="openpyxl") as xls:
            self.final_df.to_excel(
                xls, sheet_name="Provider_Locations_Final", index=False
            )
        print(f"✓ Exported → {self.output_xlsx}")
        return self.output_xlsx


# ─────────────────────────── SCRIPT MODE ─────────────────────────
if __name__ == "__main__":
    # change path below or pass via CLI
    PIPE = ProviderLocationPipeline(r"D:\Python_Task\Data2\input.xlsx")
    PIPE.run()
    PIPE.export()
