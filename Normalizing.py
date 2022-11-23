# Importing thr required libraries.
import pandas as pd
import sqlite3
from sqlite3 import Error

# To the view the database.
def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

conn_dnorm = create_connection('non_normalized.db')
sql_statement = "select * from Students;"
df = pd.read_sql_query(sql_statement, conn_dnorm)
print(df)

#     StudentID               Name         Degree                                              Exams                              Scores
# 0           1  Rodriguez, Pamela       graduate  exam7 (2017), exam9 (2018), exam3 (2018), exam...          61, 38, 85, 70, 44, 43, 68
# 1           2   Jackson, Kristie  undergraduate  exam2 (2017), exam2 (2017), exam8 (2018), exam...  66, 69, 85, 62, 92, 72, 44, 50, 23
# 2           3     Curtis, George       graduate  exam7 (2017), exam6 (2017), exam2 (2017), exam...                      68, 73, 62, 72
# 3           4    Casey, Christie  undergraduate  exam10 (2019), exam4 (2019), exam4 (2019), exa...                      41, 52, 52, 70
# 4           5       Yoder, Emily  undergraduate           exam4 (2019), exam4 (2019), exam4 (2019)                          52, 45, 61
# ..        ...                ...            ...                                                ...                                 ...
# 95         96     Dyer, Benjamin  undergraduate  exam10 (2019), exam4 (2019), exam6 (2017), exa...      47, 53, 64, 57, 71, 79, 77, 56
# 96         97     Delgado, Jason  undergraduate                         exam1 (2016), exam8 (2018)                              72, 83
# 97         98    Cortez, Kenneth  undergraduate           exam3 (2018), exam5 (2020), exam6 (2017)                          83, 55, 71
# 98         99   Short, Elizabeth  undergraduate  exam3 (2018), exam9 (2018), exam3 (2018), exam...  87, 34, 84, 75, 80, 60, 48, 55, 75
# 99        100    Andrade, Robert  undergraduate                                       exam8 (2018)                                  82

# [100 rows x 5 columns]

# We'll normalise the database by creating four tables namely, Exams, Students, Degrees, StudentExamScores.

def normalize_database(non_normalized_db_filename):

    #Creating the Degrees table.

    conn_dnorm = create_connection(non_normalized_db_filename)
    sql_deg = "SELECT DISTINCT Degree FROM Students;"
    deg = execute_sql_statement(sql_deg, conn_dnorm)
    degree = list(map(lambda row: row[0], deg))

    deg_table="""CREATE TABLE IF NOT EXISTS [Degrees](
        [Degree] TEXT NOT NULL PRIMARY KEY);"""
    def insert_degree(conn,values):
        sql="""INSERT INTO Degrees(Degree) 
        VALUES(?)"""
        cur=conn.cursor()
        cur.execute(sql,values)
        return cur.lastrowid

    conn_norm = create_connection('normalized.db',True)
    create_table(conn_norm, deg_table)

    with conn_norm:  
        for d in degree:
            insert_degree(conn_norm,(d,))

    # Creating the Exams Table.

    sql_exam="SELECT Exams from Students;"
    exa=execute_sql_statement(sql_exam, conn_dnorm)
    exam=list(map(lambda row: row[0], exa))
    duh={}
    for i in exam:
        for j in i.split(','):
            duh.update({j.strip(' ').split(' ')[0]:j.strip(' ').split(' ')[1]})

    for k,v in duh.items():
        duh[k]=v.strip('()')

    exa_table="""CREATE TABLE [Exams](
        [Exam] TEXT NOT NULL PRIMARY KEY,
        [Year] INTEGER NOT NULL);"""

    def insert_exam(conn,values):
        sql="""INSERT INTO Exams(Exam,Year)
        VALUES(?,?);"""
        cur=conn.cursor()
        cur.execute(sql,values)
        return cur.lastrowid

    conn_norm = create_connection('normalized.db')
    create_table(conn_norm,exa_table)

    with conn_norm:
        for k,v in duh.items():
                tupl=(k,v)
                insert_exam(conn_norm,tupl)

    # Creating the Students Table.

    sql_stud_deg='SELECT Name,Degree FROM Students;'
    stud_deg= execute_sql_statement(sql_stud_deg, conn_dnorm)

    stud_table="""CREATE TABLE [Students] (
        [StudentID] INTEGER NOT NULL PRIMARY KEY,
        [First_Name] TEXT NOT NULL,
        [Last_Name] TEXT NOT NULL,
        [Degree] TEXT NOT NULL,
        FOREIGN KEY (Degree) REFERENCES DEGREES(Degree)
        );"""

    def insert_stud(conn,values):
        sql="""INSERT INTO Students(First_Name, Last_Name, Degree)
        VALUES(?,?,?);"""
        cur=conn.cursor()
        cur.execute(sql,values)
        return cur.lastrowid

    conn_norm = create_connection('normalized.db')
    create_table(conn_norm,stud_table)

    with conn_norm:
        for s in stud_deg:
            fn=s[0].split(', ')[1]
            ln=s[0].split(', ')[0]
            in_tup=(fn,ln,s[1])
            insert_stud(conn_norm,in_tup)

    # Creating the StudentExamScores Table.

    sel_sql='SELECT StudentID,Exams,Scores from Students;'
    full_tab=execute_sql_statement(sel_sql,conn_dnorm)

    score_table="""CREATE TABLE [StudentExamScores](
        [PK] INTEGER NOT NULL PRIMARY KEY,
        [StudentID] INTEGER NOT NULL,
        [Exam] TEXT NOT NULL,
        [Scores] INTEGER NOT NULL
        );"""

    def insert_scores(conn,values):
        sql="""INSERT INTO StudentExamScores(StudentID, Exam, Scores)
        VALUES(?,?,?);"""
        cur=conn.cursor()
        cur.execute(sql,values)
        return cur.lastrowid

    conn_norm = create_connection('normalized.db')
    create_table(conn_norm,score_table)

    with conn_norm:
        for f,g,h in full_tab:
            score=h.split(', ')
            count=0
            for y in g.strip().split(', '):
                tup=(f,y[:-7],score[count])
                insert_scores(conn_norm,tup)
                count+=1

