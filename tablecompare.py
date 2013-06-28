#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb as db

schema  = "TABLE_SCHEMA" # set the schema in which both tables reside
table_1 = "TABLE1"       # change 
table_2 = "TABLE2"       # change

sql1 = "select * from %(schema)s.%(table_1)s" % vars()
sql2 = "select * from %(schema)s.%(table_2)s" % vars()

print "MySQL user: "
username = raw_input()

print "MySQL password: "
password = raw_input()

conn = db.connect(
    host="localhost", # change accordingly
    user=username,
    passwd=password
)

cursor1 = conn.cursor()
cursor2 = conn.cursor()

cursor1.execute(sql1)
cursor2.execute(sql2)

fieldDesc1 = cursor1.description
fieldDesc2 = cursor2.description

fieldNames1 = map(lambda x: x[0], fieldDesc1)
fieldNames2 = map(lambda x: x[0], fieldDesc2)

if fieldNames1 != fieldNames2:
    print("Campos distintos")

data1 = cursor1.fetchall()
data2 = cursor2.fetchall()

keyFieldName = "cig"
keyFieldPos = fieldNames1.index(keyFieldName)
if keyFieldPos == -1:
    keyFieldPos = 0


def num_compare(num1, num2):
    result = 0
    if num1 > num2:
        result = +1
    if num1 < num2:
        result = -1
    return result

for row in data1:
    keyFieldValue = row[keyFieldPos]
    row1 = filter(lambda x: x[0] == keyFieldValue, data1)
    row2 = filter(lambda x: x[0] == keyFieldValue, data2)
    if len(row1) > 1:
        print("La tabla %(table_1)s tiene más de una fila con clave %(keyFieldValue)s" % vars())
    if len(row2) > 1:
        print("La tabla %(table_2)s tiene más de una fila con clave %(keyFieldValue)s" % vars())
    if len(row2) == 0:
        print("La tabla %(table_2)s no contiene valores para la clave %(keyFieldValue)s" % vars())
        continue
    # Ahora nos quedamos sólo con la primera para comparar
    row1 = row1[0]
    row2 = row2[0]
    eqVector = map(
                    lambda x: x[0] == x[1],
                    zip(row1, row2)
    )
    equalRow = reduce(lambda x,y: x and y, eqVector)
    if not equalRow:
        # Find different columns
        diffCols = []
        i = 0
        while True:
            # Salimos sólo mediante pasar del último valor, y que se lance un ValueError
            try:
                diffCols.append(eqVector.index(False, i))
                i = diffCols[-1] + 1 # Last index
            except ValueError:
                break
        print ("\nDifference found on row %(keyFieldValue)s" % vars())
        for col in diffCols:
            print "Field %30s:  value1: %10s     value2: %10s" % (
                fieldNames1[col],
                row1[col],
                row2[col]
            )
    
    