# DBShadowTests
DBShadow automated tests

Developed using:
- Python 2.7.10
- nose 1.3.7
- nose-testconfig 0.10
- mysql-connector-python-cext 2.1.6


To run tests
1. Open the file config/default.ini and update with:
    - executable_path: path to the compiled dbshadow command line app
    - mysql_config_in_1: path to the mysql dbshadow in config
    - mysql_config_out_1: path to the mysql dbshadow out config
    *****NOTE:  We currently expect that the dataSource.url in the in and out configs will point to a 'dbshadow1' DB.  Our test is going to drop (if exists) and then recreate this DB
2. From the root directory of the project run:  nosetests --tc-file=config/default.ini tests/CreateTests.py