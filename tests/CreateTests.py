import unittest
from testconfig import config
from MySQLDatabaseLib import MySQLDatabaseLib
import subprocess
from subprocess import CalledProcessError


class CreateTests(unittest.TestCase):

    test_database_1 = "dbshadow1"
    test_table_1 = "people"
    test_table_2 = "peeps"
    test_table_3 = "people2"
    test_table_4 = "people3"
    test_table_5 = "people6"
    test_table_6 = "peeps2"
    dbshadow_executable = None
    mysql_in_config_1 = None
    mysql_out_config_1 = None
    mysql_db_lib = None

    def setUp(self):
        self.mysql_in_config_1 = config['dbshadow']['mysql_config_in_1']
        self.mysql_out_config_1 = config['dbshadow']['mysql_config_out_1']
        self.mysql_db_lib = MySQLDatabaseLib()
        result,message = self.mysql_db_lib.verify_mysql_config_database(self.mysql_in_config_1, self.test_database_1)
        self.assertTrue(result, message);
        result,message = self.mysql_db_lib.verify_mysql_config_database(self.mysql_out_config_1, self.test_database_1)
        self.assertTrue(result, message);

        self.mysql_db_lib.drop_test_database(self.test_database_1)
        self.mysql_db_lib.create_test_database(self.test_database_1)
        self.mysql_db_lib.create_happy_path_tables(self.test_database_1, "/Users/sharon/PycharmProjects/DBShadowTests/setup/setup.sql")
        self.dbshadow_executable = config['dbshadow']['executable_path']

    def test_01_create_mysql_to_mysql_happy_path_no_primary_keys(self):
        """Create - mysql to mysql: happy path table (same DB) - no primary keys

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -c --source people --dest peeps --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.test_table_1, '--dest', self.test_table_2, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_1)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_2)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_1)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_2)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_02_create_mysql_to_mysql_dest_table_exists_same_schema_different_data(self):
        """Create - mysql to mysql: dest table exists with same schema, different data

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
            2. Create a hapy path mysql table with the same schema and different data
        :steps:
            1. Run the command line: ./dbshadow -c --source people --dest people2 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.test_table_1, '--dest', self.test_table_3, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_1)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_3)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_1)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_3)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_03_create_mysql_to_mysql_dest_table_exists_different_schema(self):
        """Create - mysql to mysql: dest table exists with different schema

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
            2. Create a hapy path mysql table with a different schema
        :steps:
            1. Run the command line: ./dbshadow -c --source people --dest people3 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.test_table_1, '--dest', self.test_table_4, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_1)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_4)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_1)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_4)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_04_create_mysql_to_mysql_happy_path_one_primary_key(self):
        """Create - mysql to mysql: happy path table (same DB) - one primary key

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source with 1 primary key
        :steps:
            1. Run the command line: ./dbshadow -c --source people6 --dest peeps2 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.test_table_5, '--dest', self.test_table_6, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_5)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_6)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_5)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_6)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))


