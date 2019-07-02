# ###### Cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement 
from cassandra.query import SimpleStatement
  
cluster = Cluster() 
session=cluster.connect() 
 
# Create KeySpace 
try:
    query = "CREATE KEYSPACE fraud_detection_db WITH replication={'class':'SimpleStrategy','replication_factor':2};" 
    session.execute(query)
except:
    pass #print("KeySpace fraud_detection_db Already Exists")

# Session 1 - Fraud Detection 
session_1=cluster.connect('fraud_detection_db')
#print(session_1)

# Create Table and Insert Data
try:
    create_table_query = "CREATE TABLE credible(id INT PRIMARY KEY, "
    for x in header_test:
        create_table_query += x + " FLOAT, "

    create_table_query = create_table_query[:-2]+");"
    session_1.execute(create_table_query)
except:
    pass#print("Table credible already exist")

credible_insert_statement = session_1.prepare('INSERT INTO credible JSON ?') 

# Create Table and Insert Data
try:
    create_table_query = "CREATE TABLE noncredible(id INT PRIMARY KEY, "
    for x in header_test:
        create_table_query += x + " FLOAT, "

    create_table_query = create_table_query[:-2]+");"
    session_1.execute(create_table_query)
except:
    pass#print("Table noncredible already exist")

non_credible_insert_statement = session_1.prepare('INSERT INTO noncredible JSON ?') 
