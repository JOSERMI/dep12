import mysql.connector

connection = mysql.connector.connect(
    user='root', password='itguymichal', host='mysql', port="3306", database ='datapath4')
print("DB CONNECTED")

cursor = connection.cursor()
cursor.execute("select * from students")
students = cursor.fetchall()
connection.close()

print(students)