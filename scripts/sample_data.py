import os
import random
import shutil

SRC_DIR = "E:/nyc-taxi"
DST_DIR = "C:/lambda-hdfs-spark/data/sample"
FACT_FILE = "fact_trips_final.csv"
DIM_FILES = ["dim_data.csv", "dim_localizacao.csv", "dim_clima.csv"]
SAMPLE_SIZE = 100_000
RANDOM_SEED = 42

os.makedirs(DST_DIR, exist_ok=True)

# Dimension tables are tiny — copy them whole
for f in DIM_FILES:
    shutil.copy(os.path.join(SRC_DIR, f), os.path.join(DST_DIR, f))
    print(f"Copied {f}")

# Reservoir sampling on the fact table — reads line by line, no full load into RAM
src_path = os.path.join(SRC_DIR, FACT_FILE)
dst_path = os.path.join(DST_DIR, FACT_FILE)

random.seed(RANDOM_SEED)
reservoir = []

with open(src_path, "r", encoding="utf-8") as f:
    header = f.readline()
    for i, line in enumerate(f):
        if len(reservoir) < SAMPLE_SIZE:
            reservoir.append(line)
        else:
            j = random.randint(0, i)
            if j < SAMPLE_SIZE:
                reservoir[j] = line

with open(dst_path, "w", encoding="utf-8") as f:
    f.write(header)
    f.writelines(reservoir)

print(f"Sampled {len(reservoir)} rows -> {dst_path}")
