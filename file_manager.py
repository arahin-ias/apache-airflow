import glob
import os
import tarfile
from pathlib import Path

file_path = '/home/rahin/source-code/Intellij-Project/' \
            'Spark-Flights-Data-Analysis/filter_data/find_total_distance_flown'
root_file_dir = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/'
file_list = glob.iglob(root_file_dir + '**/**', recursive=True)
output_dir = '/home/rahin/output/'


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


success_files_list = filter(
    lambda x: (
        x.endswith('_SUCCESS')
    )
    , file_list
)

success_files_parents = set(map(lambda file: Path(file).parent, success_files_list))

list(map(lambda file_name: print(os.path.basename(file_name)), success_files_parents))

for f in success_files_parents:
    make_tarfile(output_dir + os.path.basename(f) + '.tar', output_dir)

