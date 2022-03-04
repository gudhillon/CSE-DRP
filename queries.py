import time
import duckdb
import numpy as np
import pyarrow.dataset as ds
import matplotlib.pyplot as plt

nyc = ds.dataset("taxi-dataset.parquet", format="parquet")
rowFilters = [69, 27, 19, 11, 9, 4, -200, None]
arrow_data = []
duckdb_data = []

for num in rowFilters:

    # Arrow
    arrow_start = time.time()
    if num is not None:
        arrow_table = nyc.to_table(filter=ds.field('total_amount') > num)
    else:
        arrow_table = nyc.to_table()
    arrow_finish = time.time()

    arrow_time = arrow_finish - arrow_start
    arrow_data.append(arrow_time)

    # DuckDB
    con = duckdb.connect()
    duck_start = time.time()
    if num is not None:
        duck_table = con.execute("SELECT * FROM nyc where total_amount > " + str(num))
    else:
        duck_table = con.execute("SELECT * FROM nyc")
    duck_finish = time.time()

    duck_time = duck_finish - duck_start
    duckdb_data.append(duck_time)


x = np.array([0,1,2,3,4,5,6,7])
y1 = np.array(arrow_data)
y2 = np.array(duckdb_data)
plt.xlabel("Percentage of Rows")
plt.ylabel("Query Latency")
my_xticks = ['1', '10', '25', '50', '75', '90', '99', '100']
plt.xticks(x, my_xticks)
plt.plot(x, y1, label = "Panda")
plt.plot(x, y2, label = "DuckDB")
plt.legend()
plt.show()
