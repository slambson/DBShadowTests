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
    dbshadow_executable = None
    mysql_in_config_1 = None
    mysql_out_config_1 = None
    mysql_db_lib = None

    def setUp(self):
        self.mysql_db_lib = MySQLDatabaseLib()
        self.mysql_db_lib.drop_test_database(self.test_database_1)
        self.mysql_db_lib.create_test_database(self.test_database_1)
        self.mysql_db_lib.create_happy_path_tables(self.test_database_1, "/Users/sharon/PycharmProjects/DBShadowTests/setup/setup.sql")
        self.dbshadow_executable = config['dbshadow']['executable_path']
        self.mysql_in_config_1 = config['dbshadow']['mysql_config_in_1']
        self.mysql_out_config_1 = config['dbshadow']['mysql_config_out_1']

    def test_01_get_person_details_valid_key(self):
        """Create - mysql to mysql: happy path table (same DB) - no primary keys

        :author: Sharon Lambson
        :component: Create - mysql to mysql
        :setup:
            1. Create a happy path mysql database for the source
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
        except CalledProcessError as e:
            self.assertTrue(False, "We got an non zero return code: {} when we ran the dbshadow app".format(e.returncode))


