import pandas as pd

# Load the CSV as raw lines
with open('corn_price_data.csv', 'r') as f:
    lines = f.readlines()

# Correct the header if needed
header = "date,weekend_or_holiday,price,return,logreturn,flag\n"
if lines[0].strip() != header.strip():
    lines[0] = header

# Fix each line to have exactly 6 columns (i.e., 5 commas)
corrected_lines = [lines[0]]  # start with header
for line in lines[1:]:
    # Remove trailing newlines and split by comma
    parts = line.strip().split(',')
    # Fill missing parts
    while len(parts) < 6:
        parts.append('')
    # Trim extra parts if any
    parts = parts[:6]
    corrected_lines.append(','.join(parts) + '\n')

# Write the corrected CSV
with open('corn_price_csv_fixed.csv', 'w') as f:
    f.writelines(corrected_lines)

print("✅ CSV fixed and saved as 'corn_price_csv_fixed.csv'")

# ACA PUEDO AÑADIR UNA CONEXION CON IB PARA DESCARGAR EL PRECIO DE CLOSE DE CADA DÍA.