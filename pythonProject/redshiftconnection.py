from pyspark.sql import SparkSession
import psycopg2
import itertools

spark = SparkSession.builder.getOrCreate()

input_bucket = "amwater-bigdata-employee-sensitive-np"
input_prefix = "successfactors-api-ingestion/202***"
df_spark = spark \
    .read \
    .format("json") \
    .option("compression", "gzip") \
    .option("header", True) \
    .load("s3://{}/{}/".format(input_bucket, input_prefix))


df_spark.write.format("parquet").mode('overwrite').save("s3://successfactornew/ouput/")
df =  spark \
    .read \
    .format("parquet") \
    .option("header", True) \
	.option ("mode", "FAILFAST")\
    .load("s3://successfactornew/ouput/*.parquet")
conn = psycopg2.connect(
    host='amwater-bigdata-redshift-prod.ceqr24sy0pgc.us-east-1.redshift.amazonaws.com',
    port='5439',
    user='data_admin',
    password='Admin@123',
    database='analytics'
)
cursor = conn.cursor()
# Define the Redshift table name
redshift_table_name = 'analytics.datalake_s.sf_employee_details_s'

columns = ['erbp_name','ethnic_group','event','event_date','event_reason','external_code','flsa_status','functional_area','gender','hr_manager_id','hr_manager_name','hrbp_name','hrc_name','is_fulltime_employee','is_lgbtq_community','is_military_spouse','job_code','job_family','job_function','job_level','job_title','last_day_worked','last_rehire_date','last_termination_date','legal_first_name','legal_last_name','location','location_address1','location_address2','location_city','location_code','location_company','location_county','location_description','location_enddate','location_externalcode','location_geozoneflx','location_group','location_name','location_name','location_standardhours','location_startdate','location_state','location_status','location_timezone','location_zipcode','manager','manager_id','marital_status','military_status','original_hire_date','pay_grade_id','pay_scale_type','pension_eligibility','permanent_address1','permanent_address2','permanent_address3','permanent_city','permanent_county','permanent_state','permanent_zipcode','position_code','position_criticality','position_entry_date','position_title','race','regrettable_turnover','termination_date','union_code','union_name','union_value','username','veteran_status']

# Iterate over each row in the DataFrame and execute the INSERT INTO statement
for index, row in df.iterrows():
    values = [row[column] for column in columns]
    insert_statement = f"INSERT INTO {redshift_table_name} (dataset_id, status, error_type, error_msg, refresh_time, duration, total_time, insert_ts) VALUES ({', '.join(['%s'] * len(columns))})"
    cursor.execute(insert_statement, values)