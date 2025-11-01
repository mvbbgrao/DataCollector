#!/usr/bin/env python3
"""
Ticker File Comparison Script
Compares two ticker files and shows what tickers are missing from the first file.
Each file should contain one ticker per line.
"""

import sys
import os
from typing import Set, List

def read_ticker_file(filename: str) -> Set[str]:
    """Read ticker file and return set of tickers (uppercase, stripped)."""
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found")
        sys.exit(1)

    tickers = set()
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                ticker = line.strip().upper()
                if ticker:  # Skip empty lines
                    tickers.add(ticker)
    except Exception as e:
        print(f"Error reading file '{filename}': {e}")
        sys.exit(1)

    return tickers

def compare_ticker_files(file1: str, file2: str):
    """Compare two ticker files and show what's missing from file1."""
    print(f"Comparing ticker files:")
    print(f"File 1: {file1}")
    print(f"File 2: {file2}")
    print("-" * 50)

    # Read both files
    tickers_file1 = read_ticker_file(file1)
    tickers_file2 = read_ticker_file(file2)

    # Find missing tickers (present in file2 but not in file1)
    missing_from_file1 = tickers_file2 - tickers_file1

    # Print summary
    print(f"Tickers in {file1}: {len(tickers_file1)}")
    print(f"Tickers in {file2}: {len(tickers_file2)}")
    print(f"Missing from {file1}: {len(missing_from_file1)}")
    print("-" * 50)

    # Print missing tickers
    if missing_from_file1:
        print("Tickers missing from first file:")
        sorted_missing = sorted(list(missing_from_file1))
        for ticker in sorted_missing:
            print(f"  {ticker}")

        # Optionally save missing tickers to file
        output_file = "missing_tickers.txt"
        with open(output_file, 'w') as f:
            for ticker in sorted_missing:
                f.write(f"{ticker}\n")
        print(f"\nMissing tickers saved to: {output_file}")
    else:
        print("No tickers are missing from the first file.")

    # Show common tickers count
    common_tickers = tickers_file1 & tickers_file2
    print(f"Common tickers: {len(common_tickers)}")
def remove_duplicates_from_file(filename: str):
    """Remove duplicate tickers from the file and save unique tickers."""
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found")
        return

    tickers = set()
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            ticker = line.strip().upper()
            if ticker:
                tickers.add(ticker)

    with open(filename, 'w', encoding='utf-8') as file:
        for ticker in sorted(tickers):
            file.write(f"{ticker}\n")
def remove_tickers_from_file(target_file: str, remove_file: str):
    """Remove tickers found in remove_file from target_file and save the result."""
    if not os.path.exists(target_file):
        print(f"Error: File '{target_file}' not found")
        return
    if not os.path.exists(remove_file):
        print(f"Error: File '{remove_file}' not found")
        return

    # Read tickers to remove
    with open(remove_file, 'r', encoding='utf-8') as f:
        remove_tickers = {line.strip().upper() for line in f if line.strip()}

    # Read target tickers
    with open(target_file, 'r', encoding='utf-8') as f:
        target_tickers = {line.strip().upper() for line in f if line.strip()}

    # Remove tickers
    updated_tickers = target_tickers - remove_tickers

    # Save updated tickers back to target_file
    with open(target_file, 'w', encoding='utf-8') as f:
        for ticker in sorted(updated_tickers):
            f.write(f"{ticker}\n")


def main():
    """Main function to run the comparison."""
    # if len(sys.argv) != 3:
    #     print("Usage: python file_compare.py <file1> <file2>")
    #     print("Example: python file_compare.py ticker_list.txt master_list.txt")
    #     sys.exit(1)

    file1 = "ticker_list.txt"
    file2 = "C:/source/MyTradingBot/data/tickers.txt"
    # remove_duplicates_from_file(file2)
    # file2 = "test_ticker_list.txt"
    compare_ticker_files(file1, file2)

    file3= "data/missing_tickers.txt"
    remove_tickers_from_file(file2, file3)

if __name__ == "__main__":
    main()