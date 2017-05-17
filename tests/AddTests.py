import unittest
from testconfig import config
from MySQLDatabaseLib import MySQLDatabaseLib
import subprocess
from subprocess import CalledProcessError


class AddTests(unittest.TestCase):

    test_database_1 = "dbshadow1"
    test_table_1 = "people"
    test_table_2 = "peeps"
    test_table_3 = "people2"
    test_table_4 = "people3"
    test_table_5 = "people4"
    test_table_6 = "people5"
    test_table_7 = "people6"
    test_table_8 = "people7"
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

    def test_01_add_mysql_to_mysql_no_existing_destination_table(self):
        """Add - mysql to mysql: empty existing destination table

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -a --source people --dest peeps --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.test_table_1, '--dest', self.test_table_2, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
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

    def test_02_add_mysql_to_mysql_empty_existing_destination_table(self):
        """Add - mysql to mysql: empty existing destination table

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
            2. Create a destination table that is empty
        :steps:
            1. Run the command line: ./dbshadow -a --source people --dest people4 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.test_table_1, '--dest', self.test_table_5, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_1)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_5)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_1)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_5)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_03_add_mysql_to_mysql_populated_existing_destination_table_no_duplicates(self):
        """Add - mysql to mysql: populated existing destination table - no duplicates

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
            2. Create a destination table that has existing data but no duplicates
        :steps:
            1. Run the command line: ./dbshadow -a --source people --dest people2 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            destination_records_start_state = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_3)
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.test_table_1, '--dest', self.test_table_3, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_1)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_3)
            expected_destination_records = destination_records_start_state + source_records
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_1)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_3)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_04_add_mysql_to_mysql_populated_existing_destination_table_with_duplicates_no_primary_keys(self):
        """Add - mysql to mysql: populated existing destination table - with duplicates no primary keys

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
            2. Create a destination table that has existing data with duplicates but no primary keys
        :steps:
            1. Run the command line: ./dbshadow -a --source people --dest people2 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            destination_records_start_state = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_6)
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.test_table_1, '--dest', self.test_table_6, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_1)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_6)
            expected_destination_records = destination_records_start_state + source_records
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_1)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_6)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_05_add_mysql_to_mysql_one_primary_key(self):
        """Add - mysql to mysql: one primary key

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source with 1 primary key
            2. Create a destination table that is empty
        :steps:
            1. Run the command line: ./dbshadow -a --source people6 --dest people7 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.test_table_7, '--dest', self.test_table_8, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_7)
            destination_records = self.mysql_db_lib.get_records_from_table(self.test_database_1, self.test_table_8)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_7)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.test_table_8)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))