import pandas as pd
from datetime import datetime
import sys

def load_sheets():
    try:
        ddh = pd.read_excel("ddh_database.xlsx")
        iet = pd.read_excel("IET_Database.xlsx")
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        sys.exit(1)
    ddh.columns = ddh.columns.str.strip()
    iet.columns = iet.columns.str.strip()
    return ddh, iet

def save_with_fallback(df, filename="ddh_database_updated.xlsx"):
    try:
        df.to_excel(filename, index=False)
        print(f"Saved: {filename}")
    except PermissionError:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt = filename.replace(".xlsx", f"_{ts}.xlsx")
        df.to_excel(alt, index=False)
        print(f"Could not overwrite '{filename}'. Saved instead as: {alt}")

def find_column(df, target_name):
    normalize = lambda s: s.strip().lower().replace("_", " ")
    for col in df.columns:
        if normalize(col) == normalize(target_name):
            return col
    return None

def main():
    ddh_df, iet_df = load_sheets()
    print("Loaded both workbooks.")

    # Normalize Co_No in both dataframes: strip whitespace, remove all spaces, and uppercase
    for df, name in [(ddh_df, 'ddh_database'), (iet_df, 'IET_Database')]:
        co_col = find_column(df, "Co_No")
        if co_col:
            df[co_col] = (
                df[co_col]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", "", regex=True)
                .str.upper()
            )
            print(f"Normalized '{co_col}' in {name}\.")
        else:
            print(f"Warning: 'Co_No' column not found in {name}; skipping normalization.")

    summary = {}

    # Fill missing Scan Type entries
    target = "Scan Type"
    scan_col = find_column(ddh_df, target)
    if scan_col:
        empties = ddh_df[scan_col].isna().sum() + (ddh_df[scan_col] == "").sum()
        ddh_df[scan_col] = ddh_df[scan_col].replace("", pd.NA).fillna("Hyperspectral VNIR-LWIR")
        summary[scan_col] = empties
        print(f"Filled {empties} missing entries in '{scan_col}'.")
    else:
        print(f"Warning: could not find a '{target}' column. Skipping scan-type fill.")

    # Determine shared columns (excluding MasterNo and Scan Type)
    exclude = {"MasterNo"}
    if scan_col:
        exclude.add(scan_col)
    shared_cols = sorted(set(ddh_df.columns).intersection(iet_df.columns) - exclude)

    # Sync values from IET into DDH
    total_changes = 0
    for col in shared_cols:
        valid = iet_df[~iet_df[col].isna()]
        unique = valid.drop_duplicates(subset="MasterNo", keep="first")
        lookup = unique.set_index("MasterNo")[col]

        changes = 0
        for idx, row in ddh_df.iterrows():
            mn = row["MasterNo"]
            if mn in lookup.index:
                new_val = lookup.loc[mn]
                old_val = row.get(col)
                if pd.isna(old_val) or old_val != new_val:
                    ddh_df.at[idx, col] = new_val
                    changes += 1
        summary[col] = changes
        total_changes += changes

    # Remove duplicate MasterNo rows
    if "MasterNo" in ddh_df.columns:
        dup_mask = ddh_df.duplicated(subset="MasterNo", keep="first")
        duplicate_mns = ddh_df.loc[dup_mask, "MasterNo"].tolist()
        before_count = len(ddh_df)
        ddh_df = ddh_df.drop_duplicates(subset="MasterNo", keep="first")
        removed_count = before_count - len(ddh_df)
        summary["Duplicates removed"] = removed_count
        if removed_count > 0:
            print("\nDuplicate MasterNo values removed:")
            for mn in dict.fromkeys(duplicate_mns):
                print(f"  {mn}")
        else:
            print("No duplicate rows to remove based on 'MasterNo'.")

    # Print summary of operations
    print("\nSummary:")
    for col, cnt in summary.items():
        if col == scan_col:
            print(f"Filled '{col}': {cnt} entries.")
        elif col == "Duplicates removed":
            print(f"\n{cnt} duplicate rows removed.")
        else:
            print(f"Synced '{col}': {cnt} replacements.")
    print(f"Total changes: {total_changes}\n")

    save_with_fallback(ddh_df)

if __name__ == "__main__":
    main()