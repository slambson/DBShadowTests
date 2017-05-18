import unittest
from testconfig import config
from MySQLDatabaseLib import MySQLDatabaseLib
import subprocess
from subprocess import CalledProcessError


class AddTests(unittest.TestCase):

    test_database_1 = "dbshadow1"
    test_database_2 = "dbshadow2"
    people_table = "people"   # Happy Path table (int, varchar(200), int schema, no PK)
    people2_table = "people2" # Happy Path table (int, varchar(200), int schema, no PK) different data
    people3_table = "people3" # Happy Path table (int, varchar(100) schema, no PK)
    people4_table = "people4" # Happy Path table (int, varchar(200), int schema, no PK) no data
    people5_table = "people5" # Happy Path table (int, varchar(200), int schema, no PK) different data
    people6_table = "people6" # Happy Path table (int, varchar(200), int schema, with PK) different data
    people7_table = "people7" # Happy Path table (int, varchar(200), int schema, with PK) no data
    peeps_table = "peeps"     # Name of the destination DB that doesn't currently exist
    dbshadow_executable = None
    mysql_in_config_1 = None
    mysql_out_config_1 = None
    mysql_setup_file_1 = None
    mysql_setup_file_2 = None
    mysql_in_config_2 = None
    mysql_out_config_2 = None
    mysql_db_lib = None

    def setUp(self):
        self.mysql_in_config_1 = config['dbshadow']['mysql_config_in_1']
        self.mysql_out_config_1 = config['dbshadow']['mysql_config_out_1']
        self.mysql_setup_file_1 = config['dbshadow']['mysql_setup_file_1']
        self.mysql_in_config_2 = config['dbshadow']['mysql_config_in_2']
        self.mysql_out_config_2 = config['dbshadow']['mysql_config_out_2']
        self.mysql_setup_file_2 = config['dbshadow']['mysql_setup_file_2']
        mysql_username = config['mysql']['mysql_username']
        mysql_password = config['mysql']['mysql_password']
        mysql_host = config['mysql']['mysql_host']
        self.mysql_db_lib = MySQLDatabaseLib(mysql_username, mysql_password, mysql_host)
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
        self.mysql_db_lib.create_test_tables(self.test_database_1, self.mysql_setup_file_1)
        self.mysql_db_lib.create_test_tables(self.test_database_2, self.mysql_setup_file_2)
        self.dbshadow_executable = config['dbshadow']['executable_path']

    def test_01_add_mysql_to_mysql_no_existing_destination_table(self):
        """Add - mysql to mysql: no existing destination table

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
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people_table, '--dest', self.peeps_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
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
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people_table, '--dest', self.people4_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people4_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people4_table)
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
            1. Should Run successfully without error.  The destination table should be the existing table data plus the source data.
        """
        try:
            destination_records_start_state = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people2_table)
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people_table, '--dest', self.people2_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people2_table)
            expected_destination_records = destination_records_start_state + source_records
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people2_table)
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
            1. Run the command line: ./dbshadow -a --source people --dest people5 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should be the existing table data plus the source data.
        """
        try:
            destination_records_start_state = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people5_table)
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people_table, '--dest', self.people5_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people5_table)
            expected_destination_records = destination_records_start_state + source_records
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people5_table)
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
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people6_table, '--dest', self.people7_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people6_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people7_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people6_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people7_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_06_add_mysql_to_mysql_dest_table_exists_different_schema(self):
        """Add - mysql to mysql: dest table exists with different schema

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
            2. Create a hapy path mysql table with a different schema
        :steps:
            1. Run the command line: ./dbshadow -a --source people --dest people3 --srcConfig mysql.in.cfg.xml --destConfig mysql.out.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            destination_records_start_state = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people3_table)
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people_table, '--dest', self.people3_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_1])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people3_table)
            expected_destination_records = destination_records_start_state + source_records
            matched,output = self.mysql_db_lib.compare_two_record_lists(expected_destination_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people3_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))

    def test_07_add_mysql_to_mysql_happy_path_different_db(self):
        """Add - mysql to mysql: happy path table (different DB)

        :author: Sharon Lambson
        :component: Add - mysql to mysql
        :setup:
            1. Create a happy path mysql table for the source
        :steps:
            1. Run the command line: ./dbshadow -a --source people --dest people4 --srcConfig mysql.in.cfg.xml --destConfig mysql.out2.cfg.xml
        :expectedResults:
            1. Should Run successfully without error.  The destination table should match the source table
        """
        try:
            output = subprocess.check_output([self.dbshadow_executable, '-a', '--source', self.people_table, '--dest', self.people4_table, '--srcConfig', self.mysql_in_config_1, '--destConfig', self.mysql_out_config_2])
            expected_output = "Committed 3 records"
            self.assertTrue(expected_output in output, "Expected the output to contain the text: '{}' but instead this was the output: {}".format(expected_output, output))
            source_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_1, self.people_table)
            destination_records = self.mysql_db_lib.get_all_records_from_table(self.test_database_2, self.people4_table)
            matched,output = self.mysql_db_lib.compare_two_record_lists(source_records, destination_records)
            self.assertTrue(matched,output)
            source_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_1, self.people_table)
            destination_schema = self.mysql_db_lib.get_schema_from_table(self.test_database_2, self.people4_table)
            matched, output = self.mysql_db_lib.compare_two_record_lists(source_schema, destination_schema)
            self.assertTrue(matched, output)
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))