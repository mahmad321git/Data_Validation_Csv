from pyspark.sql import SparkSession
import argparse
import datetime
import json


with open('C:/Users/ahmad.idrees/Desktop/Data.json') as fp:
    data = json.load(fp)

required_tables_list = data['PRISM']['gl_transaction']['required_tables']
source_files_base_path = data['PRISM']['gl_transaction']['source_files_base_path']
target_base_path = data['PRISM']['gl_transaction']['target_file_base_path']

print(required_tables_list)
print(source_files_base_path)
print(target_base_path)
print(data['PRISM']['gl_transaction']['required_tables'])

spark = SparkSession.builder.getOrCreate()


def compare_dataframe(df1, df2):
    '''
    function: compare_dataframe
    description: Compares two dataframes and show their differences.

    Args:
        df1: spark.sql.dataframe - First dataframe to be compared.
        df2: spark.sql.dataframe - Second dataframe to be compared.

    returns:
        df1subdf2: spark.sql.dataframe - Different rows of df1.
        df2subdf1: spark.sql.dataframe - Different rows of df2.
    '''
    df1subdf2 = df1.subtract(df2)
    df2subdf1 = df2.subtract(df1)

    print('Different rows in first dataframe.')
    df1subdf2.show()

    print('Different rows in second dataframe.')
    df2subdf1.show()

    return df1subdf2, df2subdf1


parser = argparse.ArgumentParser(description='Provide Details')
parser.add_argument('--batch', help='Batch')
parser.add_argument('--bucket', help='Bucket')
parser.add_argument('--table_name')
parser.add_argument('--csv1_data_path', help='File Name 1')
parser.add_argument('--csv2_data_path', help='File Name 2')
args = parser.parse_args()
print(args)

BATCH = args.batch
BUCKET = args.bucket
TABLE_NAME = args.table_name
CSV1_DATA_PATH = 'G:/csv_data/'+BUCKET+'/'+args.csv1_data_path
CSV2_DATA_PATH = 'G:/csv_data/'+BUCKET+'/'+args.csv2_data_path


UPDATED = datetime.datetime.today().replace(second=0, microsecond=0)
SUMMARY_RECORDS_COLUMNS = ['table_name', 'status', 'timestamp', 'total_row_count', 'failed_count']
INVALID_RECORDS_COLUMNS = ['table_name', 'status', 'timestamp', 'file_name', 'column_name']

print(CSV1_DATA_PATH)
print(CSV2_DATA_PATH)
# print(args)

df1 = spark.read.csv(CSV1_DATA_PATH, header=True, sep='|', inferSchema=True)
print("Showing DataFrame1:")
df1.show(2)

df2 = spark.read.csv(CSV2_DATA_PATH, header=True, sep='|', inferSchema=True)
print("Showing DataFrame2:")
df2.show(2)

df1_row_count = df1.count()
df2_row_count = df2.count()

#Column Difference logic
df1_missing_columns = []
df2_missing_columns = []
for column in df1.columns:
    if column not in df2.columns:
        df2_missing_columns.append(column)
for column in df2.columns:
    if column not in df1.columns:
        df1_missing_columns.append(column)

print("Printing the missing Columns in dataframe 1 ")
print(df1_missing_columns)

print("Printing the missing Columns in dataframe 2 ")
print(df2_missing_columns)

if not df1_missing_columns and not df2_missing_columns:
    df1, df2 = compare_dataframe(df1, df2)
    csv_a_compare_count = df1.count()
    csv_b_compare_count = df2.count()
    print(csv_a_compare_count)
    print(csv_b_compare_count)
    if csv_a_compare_count == 0 and csv_b_compare_count == 0:
        status = "Success"
        SUMMARY_RECORD_VALUES = [(TABLE_NAME, status, UPDATED, df1_row_count, 0)]
    else:
        status = "Failure"
        count = max(csv_a_compare_count, csv_b_compare_count)
        if csv_a_compare_count != 0:
            SUMMARY_RECORD_VALUES = [(TABLE_NAME, status, UPDATED, df1_row_count, count)]
            df1.write.csv("G:/csv_data/" + BUCKET + "/Result_Csv1/", header=True, sep='|')
        if csv_b_compare_count != 0:
            SUMMARY_RECORD_VALUES = [(TABLE_NAME, status, UPDATED, df2_row_count, count)]
            df2.write.csv("G:/csv_data/" + BUCKET + "/Result_Csv2/", header=True, sep='|')

    df_output = spark.createDataFrame(SUMMARY_RECORD_VALUES, SUMMARY_RECORDS_COLUMNS)
    df_output.write.csv("G:/csv_data/" + BUCKET + "/New/", header=True, sep='|')

elif df1_missing_columns:
    print("Printing the missing Columns in dataframe 1 ")
    print(df1_missing_columns)
    status = "Failure"
    SUMMARY_RECORD_VALUES = [(TABLE_NAME, status, UPDATED, str(args.csv1_data_path), str(df1_missing_columns))]
    df_output = spark.createDataFrame(SUMMARY_RECORD_VALUES, INVALID_RECORDS_COLUMNS)
    df_output.write.csv("G:/csv_data/" + BUCKET + "/Result/", header=True, sep='|')
else:
    print("Printing the missing Columns in dataframe 2 ")
    print(df2_missing_columns)
    status = "Failure"
    SUMMARY_RECORD_VALUES = [(TABLE_NAME, status, UPDATED, str(args.csv1_data_path), df1_missing_columns)]
    df_output = spark.createDataFrame(SUMMARY_RECORD_VALUES, INVALID_RECORDS_COLUMNS)
    df_output.write.csv("G:/csv_data/" + BUCKET + "/Result/", header=True, sep='|')

