#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb as db
from pprint import pprint # Pretty-printing

# Comparison configuration
schema       = "TABLE_SCHEMA" # Change to the corresponding schema
table_1      = "TABLE1"       # Change to define the first table
table_2      = "TABLE2"       # Change to define the second table
sqlTemplate  = "select * from %(schema)s.%(table)s"
tableList    = [table_1, table_2]
keyFieldName = "FIELD1"       # Change to define the display field for row differences

# Ancillary function (to use instead of lambda x: x[0])
def first(x):
    return x[0]

def is_none(x):
    return type(x) == type(None)

print "MySQL user: "
username = raw_input()

print "MySQL password: "
password = raw_input()

conn = db.connect(
    host="localhost",
    user=username,
    passwd=password
)

tableParameterDict = {}
for table in tableList:
    parameterDict = {}
    parameterDict['sql'] = sqlTemplate % {
        'schema': schema,
        'table': table
    }
    parameterDict['cursor'] = conn.cursor()
    parameterDict['numRows'] = parameterDict['cursor'].execute(parameterDict['sql'])
    parameterDict['description'] = parameterDict['cursor'].description
    parameterDict['fields'] = map(first, parameterDict['description'])
    parameterDict['data'] = parameterDict['cursor'].fetchall()
    try:
        parameterDict['keyColumn'] = parameterDict['fields'].index(keyFieldName)
    except ValueError:
        parameterDict['keyColumn'] = 0 # we try with the first
    tableParameterDict[table] = parameterDict

# Temporary list with as many "Nulls" as fields in table_1
fields1, fields2 = (
    tableParameterDict[table_1]['fields'],
    tableParameterDict[table_2]['fields']
)
tempTableFields = [None] * len(fields1)

# Sort fields in second table in the same way as in the first
for field in fields1:
    try:
        i1 = fields1.index(field)
        i2 = fields2.index(field)
        tempTableFields[i1] = fields2[i2]
    except ValueError:
        # This means field was not found in table_2
        print("Field %s not found in table %s" % (field,table_2))
        continue

# Check for missing fields (there are None entries in tempTableFields)
if len(filter(is_none, tempTableFields)) > 0:
    print("Different fields")
    for table in tableList:
        print("Fields in table %s:" % table)
        pprint(tableParameterDict[table]['fields'])

# Check if we can detect the key field; if not, we try the first field
for table in tableList:
    tableParameterDict[table]['fields'] = tempTableFields
    try:
        tableParameterDict[table]['keyColumn'] = tableParameterDict[table]['fields'].index(keyFieldName)
    except ValueError:
        tableParameterDict[table]['keyColumn'] = 0 # we try with the first


for row in tableParameterDict[table_1]['data']:
    keyFieldValue = row[tableParameterDict[table_1]['keyColumn']]
    # We process the rows for each different key column value
    keyCol1, keyCol2 = (
                            tableParameterDict[table_1]['keyColumn'],
                            tableParameterDict[table_2]['keyColumn']
    )
    row1 = filter(lambda x: x[keyCol1] == keyFieldValue, tableParameterDict[table_1]['data'])
    row2 = filter(lambda x: x[keyCol2] == keyFieldValue, tableParameterDict[table_2]['data'])
    if len(row1) > 1:
        print("Table %(table_1)s has more than one row with key %(keyFieldValue)s" % vars())
    if len(row2) > 1:
        print("Table %(table_2)s has more than one row with key %(keyFieldValue)s" % vars())
    if len(row2) == 0:
        print("Table %(table_2)s has no rows for key %(keyFieldValue)s" % vars())
        continue
    # Now we just get the first row for each result set (if there were more, we would have had a message from the prints above)
    row1 = first(row1)
    row2 = first(row2)
    # We generate a list of True or False values
    # True indicates that the field values for that column are equal,
    # False indicates they are different
    # Zip creates a list of (field1, field2) tuples for each field so that we can later compare
    # using lambda x: x[0] == x[1]
    eqVector = map(
                    lambda x: x[0] == x[1],
                    zip(row1, row2)
    )
    # equalRow is only True si _all_ elements of eqVector are True
    equalRow = reduce(lambda x,y: x and y, eqVector)
    if not equalRow:
        # Find different columns
        diffCols = []
        i = 0
        while True:
            # This loop only breaks when the index is too high, and a ValueError exception is raised
            try:
                diffCols.append(eqVector.index(False, i))
                i = diffCols[-1] + 1 # Last index
            except ValueError:
                break
        print ("\nDifference found for field %(keyFieldName)s: %(keyFieldValue)s" % vars())
        for col in diffCols:
            print "Field %30s:  value1: %10s     value2: %10s" % (
                tableParameterDict[table_1]['fields'][col],
                row1[col],
                row2[col]
            )
    
    
