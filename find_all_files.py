from spark_flight_data_dags import find_all_files

all_upload_files_list = find_all_files('/home/rahin/S3UploadData/')

tar_file = filter(
    lambda x: (
        x.endswith('tar')
    )
    , all_upload_files_list
)

count = 0
for file in tar_file:
    count += 1
    print(file)

print(count)
