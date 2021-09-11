from cassandra.cluster import Cluster
import pytest


# create connnection
def get_connection():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect("Employee")  # keyspace


# keyspace name is Employee
session = get_connection()


# Create table
def create():
    c_sql = " CREATE TABLE IF NOT EXISTS emp_details(emp_id int PRIMARY KEY, emp_name varchar,emp_salary int,base_location varchar,department varchar)"
    session.execute(c_sql)
    return 'True'


# insert data into a table(emp_details)
def insert_data():
    session.execute(
        "INSERT INTO emp_details(emp_id, emp_name, emp_salary, base_location, department) VALUES (1, 'abc',20000, 'Kolkata', 'Design')")
    session.execute(
        "INSERT INTO emp_details(emp_id, emp_name, emp_salary, base_location, department) VALUES (2, 'pqr',30000, 'Mumbai', 'Design')")
    session.execute(
        "INSERT INTO emp_details(emp_id, emp_name, emp_salary, base_location, department) VALUES (3, 'xyz',50000, 'Hyderabad', 'Design')")
    session.execute(
        "INSERT INTO emp_details(emp_id, emp_name, emp_salary, base_location, department) VALUES (4, 'mno',30000, 'Kolkata', 'Developer')")
    count = session.execute("SELECT COUNT(*) FROM emp_details")
    for i in count:
        return i.count



def select_data():
    rows = session.execute("SELECT * FROM emp_details WHERE emp_salary =50000")
    count = 0
    for row in rows:
        print(row.emp_id, row.emp_name, row.base_location, row.department)
        count += 1
    return count


# delete entire row
def delete_data():
    delete_row = session.execute("DELETE FROM emp_details WHERE emp_id = 1")
    count = session.execute("SELECT COUNT(*) FROM emp_details")
    for i in count:
        # print(i.count)
        return i.count



def update_data():
    session.execute("UPDATE emp_details SET base_location = 'Chennai' WHERE emp_id = 4")
    rows = session.execute("SELECT * FROM emp_details WHERE emp_id =4")
    for i in rows:
        return i.base_location
        #print( i.base_location)
    
create()
# pytest
def test_create():
    assert create() == 'True'


def test_insert_data():
    assert insert_data() == 4


def test_select_data():
    assert select_data() == 1


def test_delete_data():
    assert delete_data() == 3


def test_update_data():
    assert update_data() == "Chennai"