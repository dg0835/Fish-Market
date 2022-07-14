import boto3
import pandas as pd
import pprint as pp


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_name = "data-eng-resources"


def get_files_with_prefix(prefix: str):
    bucket_contents = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

    file_list = []

    for object in bucket_contents["Contents"]:
        file_list.append(object["Key"])

    return file_list


def convert_files_to_dataframe(file_list: list):

    dataframes = []

    for file in file_list:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=f"{file}")
        strbody = s3_object["Body"]
        print(strbody)

        df = pd.read_csv(strbody)
        dataframes.append(df)

    return dataframes


def combine_dataframes(dataframes):    
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df


def find_averages(df):
    grouped_df = df.groupby('Species')["Weight", "Length1", "Length2", "Length3", "Height", "Width"].mean()
    return grouped_df


file_list = get_files_with_prefix("python/fish")
dataframes = convert_files_to_dataframe(file_list)
df = combine_dataframes(dataframes)
df_averages = find_averages(df)

df_averages.to_csv("averages.csv")
s3_client.upload_file(Filename="averages.csv", Bucket=bucket_name, Key="Data30/DanielG/averages.csv")