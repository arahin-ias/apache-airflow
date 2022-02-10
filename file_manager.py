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

file_obj = tarfile.open('/home/rahin/output/zen.tar.gz', 'w:gz')

file_path = '/home/rahin/source-code/Intellij-Project/Spark-Flights-Data-Analysis/filter_data/find_total_distance_flown'

for file in glob.glob(file_path + '*.*'):
    file_obj.add(file)

file_obj.close()


def tardir(path, tar_name):
    with tarfile.open(tar_name, "w:gz") as tar_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                tar_handle.add(os.path.join(root, file))
    tar_handle.close()


tardir(file_path, '/home/rahin/output/sample.tar.gz')


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


make_tarfile('/home/rahin/output/test.tar', file_path)
