import unittest
from testconfig import config
from MySQLDatabaseLib import MySQLDatabaseLib
import subprocess
from subprocess import CalledProcessError


class CreateTests(unittest.TestCase):

    test_database_1 = "dbshadow1"
    test_database_2 = "dbshadow2"
    people_table = "people"   # Happy Path table (int, varchar(200), int schema, no PK)
    people2_table = "people2" # Happy Path table (int, varchar(200), int schema, no PK) different data
    people3_table = "people3" # Happy Path table (int, varchar(100) schema, no PK)
    people6_table = "people6" # Happy Path table (int, varchar(200), int schema, with PK) different data
    peeps_table = "peeps"     # Name of the destination DB that doesn't currently exist
    peeps2_table = "peeps2"   # Name of the destination DB that doesn't currently exist
    dbshadow_executable = None
    mysql_in_config_1 = None
    mysql_out_config_1 = None
    mysql_db_lib = None

    def setUp(self):
        self.mysql_in_config_1 = config['dbshadow']['mysql_config_in_1']
        self.mysql_out_config_1 = config['dbshadow']['mysql_config_out_1']
        self.mysql_in_config_2 = config['dbshadow']['mysql_config_in_2']
        self.mysql_out_config_2 = config['dbshadow']['mysql_config_out_2']
        self.mysql_db_lib = MySQLDatabaseLib()
        result,message = self.mysql_db_lib.verify_mysql_config_database(self.mysql_in_config_1, self.test_database_1)
        self.assertTrue(result, message);
        result,message = self.mysql_db_lib.verify_mysql_config_database(self.mysql_out_config_1, self.test_database_1)
        self.assertTrue(result, message);
        result, message = self.mysql_db_lib.verify_mysql_config_database(self.mysql_in_config_2, self.test_database_2)
        self.assertTrue(result, message);
        result, message = self.mysql_db_lib.verify_mysql_config_database(self.mysql_out_config_2, self.test_database_2)
        self.assertTrue(result, message);

        self.mysql_db_lib.drop_test_database(self.test_database_1)
        self.mysql_db_lib.create_test_database(self.test_database_1)
        self.mysql_db_lib.drop_test_database(self.test_database_2)
        self.mysql_db_lib.create_test_database(self.test_database_2)
        self.mysql_db_lib.create_test_tables(self.test_database_1, "/Users/sharon/PycharmProjects/DBShadowTests/setup/setup.sql")
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
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.people_table, '--dest', self.peeps_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.peeps_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.peeps_table)
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
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.people_table, '--dest', self.people2_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people2_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people2_table)
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
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.people_table, '--dest', self.people3_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people3_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people3_table)
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
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.people6_table, '--dest', self.peeps2_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people6_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.peeps2_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people6_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.peeps2_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_05_create_mysql_to_mysql_happy_path_different_db(self):
        """Create - mysql to mysql: happy path table (different DB)

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -c --source people --dest people --srcConfig mysql.in.cfg.xml --destConfig mysql.out2.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '--source', self.people_table, '--dest', self.people_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_2])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_2, self.people_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_2, self.people_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_06_create_mysql_to_mysql_use_select_statement_for_source_copy_everything(self):
        """Create - mysql to mysql: use select statement for source - copy everything

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -c -q "select * from people" --dest peeps --srcConfig mysql.in.cfg.xml --destConfig mysql.out2.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '-q', "select * from {}".format(self.people_table), '--dest', self.peeps_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_2])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_2, self.peeps_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_2, self.peeps_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_07_create_mysql_to_mysql_use_select_statement_for_source_copy_two_columns(self):
        """Create - mysql to mysql: use select statement for source - copy two columns

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -c -q "select id,name from people" --dest peeps --srcConfig mysql.in.cfg.xml --destConfig mysql.out2.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '-q', "select id,name from {}".format(self.people_table), '--dest', self.peeps_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_2])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            expected_destination_records = self.mysql_db_lib.get_selected_records_from_table(self.test_database_1, self.people_table, "select id,name from {}")
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_2, self.peeps_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records, max_column=2)
            self.assertTrue(matched,output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_08_create_mysql_to_mysql_use_select_statement_for_source_copy_based_on_where(self):
        """Create - mysql to mysql: use select statement for source - copy based on where

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -c -q "select id,name from people where age=38" --dest peeps2 --srcConfig mysql.in.cfg.xml --destConfig mysql.out2.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-c', '-q', "select * from {} where age=38".format(self.people_table), '--dest', self.peeps2_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_2])
            expected_output = "Committed 2 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            expected_destination_records = self.mysql_db_lib.get_selected_records_from_table(self.test_database_1, self.people_table, "select * from {} where age=38")
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_2, self.peeps2_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_2, self.peeps2_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))


