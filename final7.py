def _repair_names_from_grid(self, merged: pd.DataFrame, grid: pd.DataFrame) -> pd.DataFrame:
    """
    If merged['Middle Name'] is blank, try to pull First/Last/Middle from Grid
    using substring + swapped-name logic. Overwrite First/Last with Grid's
    values when a match is found (e.g., HARPER-ALLEN / MELANIE / LEA).
    """
    def _norm_letters(s: str) -> str:
        return re.sub(r"[^A-Z]", "", str(s).upper())

    # Build normalized reference from Grid
    g = grid[["First Name", "Last Name", "Middle Name"]].copy()
    g["FN_UP"]   = g["First Name"].astype(str).str.strip().str.upper()
    g["LN_UP"]   = g["Last Name"].astype(str).str.strip().str.upper()
    g["FN_NORM"] = g["FN_UP"].map(_norm_letters)
    g["LN_NORM"] = g["LN_UP"].map(_norm_letters)

    # Ensure merged keys are uppercase strings
    merged["First Name"]  = merged["First Name"].astype(str).str.strip().str.upper()
    merged["Last Name"]   = merged["Last Name"].astype(str).str.strip().str.upper()
    merged["Middle Name"] = merged["Middle Name"].fillna("")

    need_middle = merged["Middle Name"].eq("")
    for i in merged.index[need_middle]:
        fn = merged.at[i, "First Name"]   # roster FN (e.g., MELANIE)
        ln = merged.at[i, "Last Name"]    # roster LN (e.g., ALLEN)
        fnN = _norm_letters(fn)
        lnN = _norm_letters(ln)

        # Start with empty candidate frame so 'cand' always exists
        cand = pd.DataFrame()

        # Case A: Grid LAST == roster FIRST AND roster LAST appears in Grid FIRST
        cand = g[(g["LN_UP"] == fn) & (g["FN_NORM"].str.contains(lnN, na=False))]

        # Case B: reverse relationship
        if cand.empty:
            cand = g[(g["FN_UP"] == ln) & (g["LN_NORM"].str.contains(fnN, na=False))]

        # Case C: looser — both roster names appear across Grid FIRST/LAST
        if cand.empty:
            cand = g[g["FN_NORM"].str.contains(fnN, na=False) &
                     g["LN_NORM"].str.contains(lnN, na=False)]

        if not cand.empty:
            # ✅ Sort by length of FN_UP properly; keep inside the guard
            # Use either version depending on your pandas:
            try:
                cand = cand.sort_values(by="FN_UP", key=lambda s: s.str.len(), ascending=False)
            except TypeError:
                # Fallback for older pandas
                cand = (cand.assign(_len=cand["FN_UP"].astype(str).str.len())
                             .sort_values(by="_len", ascending=False)
                             .drop(columns="_len"))

            row = cand.iloc[0]
            merged.at[i, "First Name"]  = str(row["First Name"]).strip().upper()
            merged.at[i, "Last Name"]   = str(row["Last Name"]).strip().upper()
            merged.at[i, "Middle Name"] = str(row["Middle Name"] or "").strip().upper()

    return merged
