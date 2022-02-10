import glob
import os
from pathlib import Path

root_file_dir = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
file_list = glob.iglob(root_file_dir + '**/**', recursive=True)

list(
    map(lambda x: print(x),

        file_list)
)

# print(filename)
# print(Path(filename).parent)

# entries = os.listdir(root_file_dir)
