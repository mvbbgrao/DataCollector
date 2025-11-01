import os

# Folder containing your .txt files
folder_path = r"C:\source\MyTradingBot\data\ticker_backups"

# Output file
output_file = "combined_tickers.txt"

# Set to store unique tickers
tickers = set()

# Loop through all .txt files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".txt"):
        file_path = os.path.join(folder_path, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                ticker = line.strip().upper()  # normalize to uppercase
                if ticker:
                    tickers.add(ticker)

# Sort tickers alphabetically (optional)
sorted_tickers = sorted(tickers)

# Write to output file
with open(output_file, "w", encoding="utf-8") as out:
    out.write("\n".join(sorted_tickers))

print(f"✅ Combined {len(sorted_tickers)} unique tickers into: {output_file}")
