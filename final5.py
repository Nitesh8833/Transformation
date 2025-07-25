    # ---------- MERGE & POLISH ----------
    def _build_final(self, prov: pd.DataFrame, loc: pd.DataFrame) -> pd.DataFrame:
        """
        • 1️⃣ exact merge  Location ↔ LOC DBA NAME
        • 2️⃣ for rows still missing Street Address:
              a. find all location rows that share the same Street‑Address
                 *fingerprint*   (first 10 alphanum chars)
              b. if >1 such rows, keep the ones whose LOC DBA NAME
                 contains **every synonym‑normalised word** from the
                 provider’s Location string (ER≡er, Surgery≡Surg, OR)
              c. if that leaves:
                    • exactly 1 → copy it
                    • 0 or >1  → copy the **first** fingerprint hit
              d. if fingerprint set empty, fall back to naive substring
                 search and copy the first hit
        • 3️⃣ dedup headers, prettify date columns, move affiliation last
        """

        # 1️⃣ exact merge --------------------------------------------------
        final = prov.merge(
            loc, left_on="Location", right_on="LOC DBA NAME",
            how="left", suffixes=("", "_loc")
        ).drop(columns=["LOC DBA NAME"])

        # columns that exist only in loc → to be copied
        addr_cols = [c for c in loc.columns if c not in prov.columns]

        # helper: address fingerprint (first 10 alphanum chars, lowercase)
        fp = lambda s: re.sub(r"[^a-z0-9]", "", str(s).lower())[:10]

        # iterate over rows still blank after exact merge
        for idx in final.index[final["Street Address"].isna()]:
            loc_frag   = str(final.at[idx, "Location"]).lower().strip()
            addr_fp    = fp(final.at[idx, "Primary Location Address"])

            # (a) all rows whose Street Address starts with same fp
            fp_hits = loc[loc["Street Address"].str.lower()
                          .str.replace(r"[^a-z0-9]", "", regex=True)
                          .str.startswith(addr_fp, na=False)]

            # (b) reduce with synonym‑aware word filter
            best_hits = fp_hits
            if len(fp_hits) > 1 and loc_frag:
                words = {self._norm_word(w) for w in re.split(r"\W+", loc_frag) if w}

                def keep(dba: str) -> bool:
                    dba_words = {self._norm_word(w) for w in re.split(r"\W+", dba)}
                    return words.issubset(dba_words)

                best_hits = fp_hits[
                    fp_hits["LOC DBA NAME"].apply(
                        lambda s: keep(str(s)) if pd.notna(s) else False
                    )
                ]

            # choose the “winner” row -------------------------------------
            if len(best_hits) == 1:                # unique after filter
                winner = best_hits.iloc[0]
            elif len(fp_hits) >= 1:                # ambiguous → take first fp match
                winner = fp_hits.iloc[0]
            else:                                  # fingerprint failed → substring
                sub = loc[loc["LOC DBA NAME"].str.lower()
                           .str.contains(loc_frag, na=False)].head(1)
                if sub.empty:
                    continue                       # give up, leave blank
                winner = sub.iloc[0]

            # copy address / org columns to the blank provider row
            for col in addr_cols:
                final.at[idx, col] = winner[col]

        # 3️⃣ housekeeping -------------------------------------------------
        final.columns = self._dedup_columns(list(final.columns))
        final = self._fmt_date_cols(final)

        if "Hospital Affiliation" in final.columns:
            final = final[[c for c in final.columns if c != "Hospital Affiliation"]
                          + ["Hospital Affiliation"]]

        return final
