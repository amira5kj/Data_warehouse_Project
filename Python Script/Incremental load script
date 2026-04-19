"""
Sales CSV Auto-Loader
======================
This script automatically loads sales CSV files into a SQL Server database table called 'sales' (all_data).

HOW IT WORKS:
- It scans a folder for any CSV files named like "Sales YYYY.csv"
- For PREVIOUS years  > bulk insert all rows at once (fast, since data won't change)
- For CURRENT year    > check each row individually and only insert NEW rows (safe, since data grows)
- It tracks which files have already been fully loaded so it never re-processes them
- It uses Order_ID as the unique key to detect duplicate rows
"""
import os
import re
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# 1. CONFIGURATION  
SALES_FOLDER = r"D:\1-data analysis\Second project\sales"

# SQL Server connection string
DB_CONNECTION = (
    "mssql+pyodbc://@AMORA\\SQLEXPRESS/sales"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
    "&TrustServerCertificate=yes"
)

TABLE_NAME = "all_data"   # Name of the table in the database
TRACKER_FILE = os.path.join(SALES_FOLDER, "loaded_files_tracker.json") # File to track which files have already been fully loaded (previous-year files)

# 2.Load the tracker file
def load_tracker():
    """
    The tracker is a simple JSON file that stores a list of filenames
    that have already been fully bulk-inserted (previous-year files).
    Example: ["Sales 2014.csv", "Sales 2015.csv"]
    """
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, "r") as f:
            return json.load(f)
    return []  # Empty list if tracker doesn't exist yet


def save_tracker(loaded_files):
    """Save the updated list of fully loaded files back to the tracker."""
    with open(TRACKER_FILE, "w") as f:
        json.dump(loaded_files, f, indent=2)

# 3.Read and clean a CSV file
def read_csv(filepath):
    """
    Reads a sales CSV file and maps its columns to the database column names.
    Also converts date columns to proper datetime format.
    """
    df = pd.read_csv(filepath)

    # Rename CSV columns to database column names
    df = df.rename(columns={
        "Order ID":       "Order_ID",
        "Order Date":     "Order_Date",
        "Ship Date":      "Ship_Date",
        "Ship Mode":      "Ship_Mode",
        "Customer ID":    "Customer_ID",
        "Customer Name":  "Customer_Name",
        "Postal Code":    "Postal_Code",
        "Sub-Category":   "Sub_Category",
        "Product ID":     "Product_ID",
        "Product Name":   "Product_Name",
    })

    # Drop the "Row ID" column — we don't need it in the database
    if "Row ID" in df.columns:
        df = df.drop(columns=["Row ID"])

    # Convert Excel serial numbers to real dates
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], unit="D", origin="1899-12-30", errors="coerce")
    df["Ship_Date"]  = pd.to_datetime(df["Ship_Date"],  unit="D", origin="1899-12-30", errors="coerce")
    return df

# 4. STRATEGY A: Bulk Insert (for previous years)
def bulk_insert(df, engine, filename):
    """
    Inserts ALL rows from the dataframe into the database at once.
    Used for previous-year files where data is complete and won't change.
    This is fast because it sends everything in one operation.
    """
    print(f"Bulk inserting {len(df)} rows from '{filename}'...")
    df.to_sql(
        name=TABLE_NAME,
        con=engine,
        if_exists="append",   # Add to existing table, don't replace it
        index=False,           # Don't write the pandas row index as a column
        chunksize=1000         # Send 1000 rows at a time to avoid memory issues
    )
    print(f"Done! {len(df)} rows inserted.")

# 5. STRATEGY B: Incremental Insert (for current year)
def incremental_insert(df, engine, filename):
    """
    For the current year's file, only insert rows where Order_Date == today.
    Rows from previous days are skipped to prevent duplication,
    since they were already inserted when the script ran on those days.
    """
    print(f"Checking for new rows in '{filename}' (current year)...")

    # Step 1: Get today's date (no time component, just the date)
    today = pd.Timestamp(datetime.now().date())
    print(f"Today's date: {today.date()}")

    # Step 2: Make sure Order_Date is in datetime format (in case it isn't already)
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")

    # Step 3: Keep ONLY rows where Order_Date matches today
    # - Rows from previous days, already inserted before, skip them
    # - Rows from today, new data, insert them
    todays_rows = df[df["Order_Date"].dt.date == today.date()]

    print(f"Found {len(todays_rows)} row(s) with today's date in the file.")

    # Step 4: Insert only today's rows
    if len(todays_rows) == 0:
        print(f" No new rows to insert for today.")
    else:
        print(f" Inserting {len(todays_rows)} new rows...")
        todays_rows.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )
        print(f" Done! {len(todays_rows)} new rows inserted.")

# 6. Scan folder and process files
def main():
    current_year = datetime.now().year

    print("=" * 55)
    print("  Sales Auto-Loader Starting")
    print(f"  Current Year: {current_year}")
    print("=" * 55)

    # Connect to the database
    print("\nConnecting to database...")
    engine = create_engine(DB_CONNECTION)
    print(" Connected.\n")

    # Load the tracker (list of already-fully-loaded files)
    loaded_files = load_tracker()
    print(f"Already loaded files (skipping): {loaded_files}\n")

    # Scan the folder for CSV files matching "Sales YYYY.csv"
    all_csv_files = [
        f for f in os.listdir(SALES_FOLDER)
        if re.match(r"Sales \d{4}\.csv", f, re.IGNORECASE)
    ]

    if not all_csv_files:
        print("No sales CSV files found in the folder. Exiting.")
        return

    print(f"Found {len(all_csv_files)} sales file(s): {all_csv_files}\n")

    # Process each file
    for filename in sorted(all_csv_files):  # sorted = chronological order

        # Extract the year from the filename (e.g., "Sales 2015.csv" → 2015)
        year_match = re.search(r"\d{4}", filename)
        if not year_match:
            print(f"  Skipping '{filename}' — couldn't extract year.")
            continue

        file_year = int(year_match.group())
        filepath = os.path.join(SALES_FOLDER, filename)

        print(f"Processing: {filename}  (year={file_year})")

        # PREVIOUS YEAR FILE
        if file_year < current_year:

            # Skip if already bulk-loaded before
            if filename in loaded_files:
                print(f"  ✓ Already loaded previously. Skipping.\n")
                continue

            # Read and bulk insert
            df = read_csv(filepath)
            bulk_insert(df, engine, filename)

            # Mark this file as done so we never load it again
            loaded_files.append(filename)
            save_tracker(loaded_files)

        # CURRENT YEAR FILE 
        elif file_year == current_year:
            # Always re-check for new rows (don't skip, don't mark as "done")
            df = read_csv(filepath)
            incremental_insert(df, engine, filename)

        # FUTURE YEAR FILE (shouldn't happen, but just in case) ──
        else:
            print(f" File year {file_year} is in the future. Skipping.\n")
            continue

        print()  # Blank line between files

    print("=" * 55)
    print("  All done!")
    print("=" * 55)


if __name__ == "__main__":
    main()
