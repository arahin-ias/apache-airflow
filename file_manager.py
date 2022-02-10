import glob
import os
import tarfile
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

filename = '/home/rahin/output/first.tar'

file_obj = tarfile.open(filename, 'w')

list_files_in_dir = os.listdir(
    '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/find_total_distance_flown')

for f in list_files_in_dir:
    file_obj.add(f)

file_obj.close()
