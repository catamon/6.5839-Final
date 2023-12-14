import csv

file_name = f"analysis_tables/sorted_combinations/disease_indicators_sorted.csv"
new_reader = []
with open(file_name, "r") as file:
    reader = csv.reader(file)

    for row in reader:
        extra = ["" for _ in range(17 - len(row))]
        decimals = [row[0]]
        for i, el in enumerate(row[1:]):
            if i % 2 == 1:
                decimals.append(round(float(el), 2))
            else:
                decimals.append(el)
        new_reader.append(decimals + extra)

header = ["query_key"]
for i in range(8):
    header.append(f"{i} hybrid_key")
    header.append(f"{i} time")

print(header)

new_reader = [header] + new_reader
print(new_reader[2])

csv_file_path = f"DISeas.csv"
with open(csv_file_path, "w", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerows(new_reader)


"""
img1 17 electric

"""
