from cassandra_db import *

print("\n##################")
print("# Credible Data  #")
print("##################\n")
rows=session_1.execute('SELECT * FROM credible')
for row in rows:
    print(row)
    print("\n"+"="*50 + "\n")

print("\n######################")
print("# Non Credible Data  #")
print("######################\n")

# #### Fetch Non Credible Data

# Extract Data
rows=session_1.execute('SELECT * FROM noncredible')
for row in rows:
    print(row)
    print("\n"+"="*50+"\n")
