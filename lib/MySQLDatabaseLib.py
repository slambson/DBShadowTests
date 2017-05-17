import mysql.connector
from mysql.connector import errorcode


class MySQLDatabaseLib:

    def __init__(self):
        pass

    def _get_connection_to_database(self, database):
        """Get a connection to the specified database

        :param database:
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

        :param database:
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


    def create_happy_path_tables(self, database, sql_file):
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

    def select_all_from_table(self, database, table):
        try:
            connection = self._get_connection_to_database(database)
            cursor = connection.cursor()

            query = ("SELECT * FROM {}".format(table))
            cursor.execute(query)

            for (id, name, age) in cursor:
                print("{}: {} - {}".format(id, name, age))

        except Exception as err:
            raise Exception("Failed creating test table {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def get_records_from_table(self, database, table):
        try:
            connection = self._get_connection_to_database(database)
            cursor = connection.cursor()

            query = ("SELECT * FROM {}".format(table))
            cursor.execute(query)
            result = cursor.fetchall()
        except Exception as err:
            raise Exception("Failed creating test table {}: {}".format(database, err))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()
        return result

    def compare_two_record_lists(self, record_list_1, record_list_2):
        if len(record_list_1) != len(record_list_2):
            return False,"Record size were not the same.  Source size was: {}  Destination size was: {}".format(len(record_list_1), len(record_list_2))

        for index in range(len(record_list_1)):
            id_1 = record_list_1[index][0]
            id_2 = record_list_2[index][0]
            if id_1 != id_2:
                return False, "Record mismatch: for row: {} id: {} not equal: {}".format(index, id_1, id_2)
            name_1 = record_list_1[index][1]
            name_2 = record_list_2[index][1]
            if name_1 != name_2:
                return False, "Record mismatch: for row: {} name: {} not equal: {}".format(index, name_1, name_2)
            age_1 = record_list_1[index][2]
            age_2 = record_list_2[index][2]
            if age_1 != age_2:
                return False, "Record mismatch: for row: {} age: {} not equal: {}".format(index, age_1, age_2)

        return True, "All records matched!"