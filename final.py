from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import psycopg2
#Function reads data employee from s3 bucket
def read(input_paths):
    df_succes = spark \
        .read \
        .format("json") \
        .option("compression", "gzip") \
        .option("header", True) \
        .load(input_paths)
    return df_succes
#Function find and update purged employee data
def find_purged_ids(df_data):
    # Check DRTM_node_name for any purge information
    df_n = df_data.select('employee_id','DRTM_node_name','address_information','national_id_card','primary_phone_number','business_email_address','pay_scale_type','original_hire_date','termination_date','legal_first_name','legal_last_name','gender','marital_status').where(df_data.DRTM_node_name != 'null')
    # Null particular column values based on information in DRTmnode_name column
    updated_df = df_n.withColumn("address_information",when(df_n["DRTM_node_name"].contains("address_information")| df_n["DRTM_node_name"].contains("person_relation"), lit(None)).otherwise(df_n["address_information"]))
    df1 = updated_df.withColumn("national_id_card",when(df_n["DRTM_node_name"].contains("national_id_card") | df_n["DRTM_node_name"].contains("person_relation"), lit(None)).otherwise(df_n["national_id_card"]))
    return df1

#Function updates record of purged employee from s3 bucket
def update_received_data(df_data,df_purged_employee):
    join_columns = ['employee_id','DRTM_node_name','address_information','national_id_card','primary_phone_number','business_email_address','pay_scale_type','original_hire_date','termination_date','legal_first_name','legal_last_name','gender','marital_status']
    new_df = df_data.join(df1, on=join_columns, how='left')
    return new_df
#Move data to redshift
def redshift_data():
    dbname = "analytics"
    user = "admin"
    password = "Admin123"
    host = "amwater-bigdata-redshift-np.cl084jmkvw1w.us-east-1.redshift.amazonaws.com"
    port = "5439"
    s3_path = "s3://successfactornew/out/output.parquet/"
    # Connect to Redshift
    conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
    )
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS analytics.datalake_s.sf_employee_details_s_tr(
    employee_id varchar(20),
    legal_first_name varchar(40),
    legal_last_name varchar(40),
    employment_staus varchar(5),
    company_code varchar(20),
    company varchar(40),
    division_code varchar(20),
    division varchar(40),
    location varchar(50),
    union_value varchar(50),
    union_code varchar(20),                     
    union_name varchar(50),
    employee_group varchar(50),
    is_fulltime_employee varchar(30),
    cost_center_code varchar(30),
    cost_center varchar(50),
    department_code varchar(10),
    department varchar(30),
    job_code varchar(20),
    job_title varchar(30),
    position_code varchar(20),
    position_title varchar(30),
    flsa_status varchar(10),
    original_hire_date varchar(40),
    aw_hire_date varchar(40),
    last_rehire_date varchar(40),
    last_day_worked varchar(40),
    termination_date varchar(40),               
    date_of_death varchar(40),
    event varchar(40),
    event_reason varchar(40),
    event_date varchar(40),
    gender varchar(10),
    ethnic_group varchar(40),
    race varchar(30),
    military_status varchar(50),
    veteran_status varchar(50),
    disiability_status varchar(50),             
    eeo_category varchar(50),
    amount varchar(20),
    annualized_salary_amount varchar(30),       
    date_of_birth varchar(30),                 
    pay_grade_id varchar(30),
    pay_scale_type varchar(30),
    manager_id varchar(20),                     
    manager varchar(50),                      
    hr_manager_id varchar(20),
    hr_manager_name varchar(50),               
    job_family varchar(30),
    aap_establishment varchar(30),             
    direct_report varchar(60),                 
    regretable_turnover varchar(60),
    job_level varchar(30),
    position_criticality varchar(40),
    job_function varchar(70),                    
    pension_eligibility varchar(40),           
    is_military_spouse varchar(20),             
    is_lgbtq_community varchar(40),
    position_entry_date varchar(30),            
    business_email_address varchar(50),         
    username varchar(30),                       
    location_code varchar(30), 
    bussiness_unit_code varchar(30),           
    bussiness_unit_name varchar(60),           
    external_code varchar(30),                 
    functionl_area varchar(40),   
    marital_staus varchar(20),                 
    last_termination_date varchar(30),         
    location_name varchar(40),                 
    address varchar(200),            
    city varchar(100),
    state varchar(100),                         
    zip_code varchar(20));
    
    """
    cursor.execute(create_table_query)
    
    load_data_in_table = """copy analytics.datalake_s.sf_employee_details_s_tr from '{}' iam_role 'arn:aws:iam::462393762422:role/RedshiftRoleMDMS' FORMAT AS PARQUET;""".format(s3_path)

    cursor.execute(load_data_in_table)
    
    cursor.execute("commit;")
    
    conn.commit()
    
    conn.close()
    
# Main code    

if __name__ == "__main__":
    # Input for employee data 
    input_paths="s3://amwater-bigdata-employee-sensitive-np/successfactors-api-ingestion/202***/*.json.gz"
    
    # Set Spark session
    
    spark = SparkSession.builder \
        .appName("s3 to Spark") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.driver.extraClassPath", "jdbc:redshift://'amwater-bigdata-redshift-prod.ceqr24sy0pgc.us-east-1.redshift.amazonaws.com")\
        .enableHiveSupport() \
        .getOrCreate()
     
    
    print("################################All Received Data###################################################")
    df_data = read(input_paths)
    df_data.show()
    print("############################Showing Purged Employee Data############################################")
    df_purged_employee = find_purged_ids(df_data)
    df_purged_employee.show()
    purged_employee = df_purged_employee.select("employee_id").rdd.flatMap(list).collect()
    print("Purged Employee Ids:",purged_employee)
    
    print("###########################Showing Employee Data with purged employee################################")
    final_df=update_received_data(df_data,df_purged_employee)
    final_df_selected columns = new_df.select('employee_id','legal_first_name','legal_last_name','employment_status','company_code','company','division_code','division','location','union_value','union_code','union_name','employee_group','is_fulltime_employee','cost_center_code','cost_center','department_code','department','job_code','job_title','position_code','position_title','flsa_status','original_hire_date','aw_hire_date','last_rehire_date','last_day_worked','termination_date','date_of_death','event','event_reason','event_date','gender','ethnic_group','race','military_status','veteran_status','disability_status','eeo_category_1','amount','annualized_salary_amount','date_of_birth','pay_grade_id','pay_scale_type','manager_id','manager','hr_manager_id','hr_manager_name','job_family','aap_establishment','direct_reports','regrettable_turnover','job_level','position_criticality','job_function','pension_eligibility','is_military_spouse','is_lgbtq_community','position_entry_date','business_email_address','username','location_code','business_unit_code','business_unit_name','external_code','functional_area','marital_status','last_termination_date','location_name','address','city','state','zip_code')
    final_df_selected.write.format("parquet").mode('overwrite').save("s3://successfactornew/out/output.parquet")
    
    print("####################################Move data employee to Redshift###################################")
    redshift_data()
    print("Data copied successfully!")
    print("##########################################Done#######################################################")