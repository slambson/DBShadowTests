import mysql.connector
from mysql.connector import errorcode


class MySQLDatabaseLib:

    def __init__(self):
        pass

    def _get_connection_to_database(self, database):
        """Get a connection to the specified database

        :param database: The test database name to get a connection to
        :type database: str
        :return: MySQLConnection
        """
        try:
            connection = mysql.connector.connect(user='root', password='testit',
                                             host='127.0.0.1',
                                             database=database)
        except Exception as err:
            raise Exception("Failed getting connection to database {}: {}".format(database, err))

        return connection

    def drop_test_database(self, database):
        """Drop the specified database

        :param database: The test database name to drop
        :type database: str
        """
        try:
            connection = mysql.connector.connect(user='root', password='testit',
                                                 host='127.0.0.1')
            cursor = connection.cursor()
            cursor.execute("DROP DATABASE {}".format(database))
            cursor.close()
            connection.close()
        except Exception as err:
            print ("Failed deleting test database {}: {}".format(database, err))
            pass
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def create_test_database(self, database):
        """Create the specified database

        :param database: The test database name to create
        :type database: str
        """
        try:
            connection = mysql.connector.connect(user='root', password='testit',
                                                 host='127.0.0.1')
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database))
        except Exception as err:
            raise Exception("Failed creating test database {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


    def create_test_tables(self, database, sql_file):
        """Create the test database tables

        :param database: The test database name create tables in
        :type database: str
        :param sql_file: The sql file that contains the table creation and population statements
        :type sql_file str
        """
        try:
            connection = self._get_connection_to_database(database)
            cursor = connection.cursor()
            fd = open(sql_file, 'r')
            sqlFile = fd.read()
            fd.close()
            sqlCommands = sqlFile.split(';')

            for command in sqlCommands:
                try:
                    if command.strip() != '':
                        cursor.execute(command)
                except IOError, msg:
                    print "Command skipped: ", msg
            connection.commit()
        except Exception as err:
            raise Exception("Failed creating test table {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


    def get_selected_records_from_table(self, database, table, query):
        """Get selected records from the table based on the select query

        :param database: The test database to query
        :type database: str
        :param table: The table to query
        :type table: str
        :param query: The SQL query with the table as a {} in the string that we will replace in the method
        :type query: str
        :return result: A list of the database query results
        :rtype result: List
        """
        try:
            connection = self._get_connection_to_database(database)
            cursor = connection.cursor()

            query = (query.format(table))
            cursor.execute(query)
            result = cursor.fetchall()
        except Exception as err:
            raise Exception("Failed getting records for test table {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()
        return result

    def get_all_records_from_table(self, database, table):
        """Get all the records from the table

        :param database: The test database to query
        :type database: str
        :param table: The table to query
        :type table: str
        :return result: A list of the database query results
        :rtype result: List
        """
        try:
            connection = self._get_connection_to_database(database)
            cursor = connection.cursor()

            query = ("SELECT * FROM {}".format(table))
            cursor.execute(query)
            result = cursor.fetchall()
        except Exception as err:
            raise Exception("Failed getting records for test table {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()
        return result

    def get_schema_from_table(self, database, table):
        """Get schemarecords from the table

        :param database: The test database to query
        :type database: str
        :param table: The table to query
        :type table: str
        :return result: A list of the database query schema results
        :rtype result: List
        """
        try:
            connection = self._get_connection_to_database(database)
            cursor = connection.cursor()

            query = ("DESC {}".format(table))
            cursor.execute(query)
            result = cursor.fetchall()
        except Exception as err:
            raise Exception("Failed getting schema for test table {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()
        return result

    def compare_two_record_lists(self, record_list_1, record_list_2, check_columns=["id", "name", "age"]):
        """Compare two lists of database query records

        :param record_list_1: The source record list
        :type record_list_1: List
        :param record_list_2: The destination record list
        :type record_list_2: List
        :param check_columns: List of the columns to check.  List should only contain at most ["id", "name", "age"]
        :type check_columns: List
        :return tuple: boolean on whether all the records match and a string containing message details
        :rtype (bool, str)
        """
        if len(record_list_1) != len(record_list_2):
                return False,"Record size were not the same.  Source size was: {}  Destination size was: {}".format(len(record_list_1), len(record_list_2))

        for index in range(len(record_list_1)):
            if "id" in check_columns:
                id_1 = record_list_1[index][0]
                id_2 = record_list_2[index][0]
                if id_1 != id_2:
                    return False, "Record mismatch: for row: {} id: {} not equal: {}".format(index, id_1, id_2)
            if "name" in check_columns:
                name_1 = record_list_1[index][1]
                name_2 = record_list_2[index][1]
                if name_1 != name_2:
                    return False, "Record mismatch: for row: {} name: {} not equal: {}".format(index, name_1, name_2)
            if "age" in check_columns:
                age_1 = record_list_1[index][2]
                age_2 = record_list_2[index][2]
                if age_1 != age_2:
                    return False, "Record mismatch: for row: {} age: {} not equal: {}".format(index, age_1, age_2)

        return True, "All records matched!"

    def verify_mysql_config_database(self, file, test_db):
        """Verify that the mysql database config specifies the expected database that our tests rely on

        :param file: The mysql database config file
        :type file: str
        :param test_db: The database name that is expected to be in the dataSource.url field
        :type test_db: str
        :return tuple: boolean on whether all the database name is corrrect in the database config and a string containing message details
        :rtype (bool, str)
        """
        f = open(file)
        for line in iter(f):
            if "dataSource.url" in line:
                if "/{}<".format(test_db) not in line:
                    f.close()
                    return False,"CANNOT RUN TEST: DB config file: {} does not have the correct DB: {} specified for the dataSource.ulr.  Please correct file config.".format(file, test_db)
                else:
                    break
        f.close()
        return True,"Config DB is correct"
