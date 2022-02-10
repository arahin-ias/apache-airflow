import glob
import os
from pathlib import Path

root_file_dir = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
file_list = glob.iglob(root_file_dir + '**/**', recursive=True)

success_files_list = filter(
    lambda x: (
        x.endswith('_SUCCESS')
    )
    , file_list
)

success_files_parents = set(map(lambda file: Path(file).parent, success_files_list))

list(map(lambda x: print(x), success_files_parents))

# print(filename)
# print(Path(filename).parent)

# entries = os.listdir(root_file_dir)
