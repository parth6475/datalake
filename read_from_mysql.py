import mysql.connector

# Set up the connection
cnx = mysql.connector.connect(user='root', password='0101',host='127.0.0.1',database='datalake')
cursor = cnx.cursor()

# Execute an SQL command
cursor.execute("SELECT u_name FROM user")
all_user = cursor.fetchall()

data=[]
# Fetch the results
for result in all_user:
    data.append(result[0])

cursor.execute("SELECT access_name FROM permission WHERE u_name=%s", ("asd",))
access_user = cursor.fetchall()

data2=[]
# Fetch the results
for result in access_user:
    data2.append(result[0])

for s in data2:
    if s in data:
        data.remove(s)
# Close the connection
print(data)
print(data2)
cursor.close()
cnx.close()
