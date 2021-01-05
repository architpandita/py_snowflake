import snowflake.connector
import json

with open("cred.json","r") as f:
    cred = json.load(f)

conn = snowflake.connector.connect(
    user=cred["userid"],
    password=cred["password"],
    account=cred["account"],
    session_parameters={
            "QUERY_TAG": "EndOfMonthFinance",
    }
)
"""
You can also set session parameters by executing the SQL statement ALTER SESSION SET ... after connecting:

con.cursor().execute("ALTER SESSION SET QUERY_TAG = 'EndOfMonthFinancials'")


"""
data = {}
i = 0
data.update({i: "Connection Close script end..."})
i += 1

print("success in connecting")
conn.cursor().execute("use role accountadmin")

# Creating a Database, Schema, and Warehouse
conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
conn.cursor().execute("USE DATABASE testdb_mg")
conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")

# Using the Database, Schema, and Warehouse
conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
conn.cursor().execute("USE DATABASE testdb_mg")
conn.cursor().execute("USE SCHEMA testdb_mg.testschema_mg")


# Creating Tables and Inserting Data

conn.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "test_table(col1 integer, col2 string)")

conn.cursor().execute(
    "INSERT INTO test_table(col1, col2) VALUES " +
    "    (123, 'indian Cricket'), " +
    "    (100, 'Kapil Dev')")


# Putting Data
conn.cursor().execute("PUT file://./data/crick* @testdb_mg.testschema_mg.%test_table")


conn.cursor().execute("""COPY INTO test_table from @testdb_mg.testschema_mg.%test_table/crick1.csv.gz
                      file_format = (type = csv field_delimiter=',')
                        pattern = '.*.csv.gz'
                        on_error= 'skip_file'""")

# For S3

# Copying Data
# con.cursor().execute("""
# COPY INTO testtable FROM s3://<your_s3_bucket>/data/
#     CREDENTIALS = (
#         aws_key_id='{aws_access_key_id}',
#         aws_secret_key='{aws_secret_access_key}')
#     FILE_FORMAT=(field_delimiter=',')
# """.format(
#     aws_access_key_id=AWS_ACCESS_KEY_ID,
#     aws_secret_access_key=AWS_SECRET_ACCESS_KEY))


# Querying Data
cur = conn.cursor()

try:
    cur.execute("SELECT col1, col2 FROM test_table ORDER BY col1")
    for (col1, col2) in cur:
        print('{0}, {1}'.format(col1, col2))
        data.update({i: '{0}, {1}\n'.format(col1, col2)})
        i += 1
finally:
    cur.close()

# Use fetchone or fetchmany if the result set is too large to fit into memory.

results = conn.cursor().execute("SELECT col1, col2 FROM test_table").fetchall()


for rec in results:
    print('%s, %s' % (rec[0], rec[1]))
    data.update({i: '%s, %s \n' % (rec[0], rec[1])})
    i += 1

conn.close()
data.update({i: "Connection Close script end..."})

with open("out_record.json","w") as f1:
    json.dump(data,f1)
print("Connection Close script end")
