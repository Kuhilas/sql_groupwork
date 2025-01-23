from configparser import ConfigParser
import os

def config(filename='C:/tuukka/sql_groupwork/SQL_groupwork/src/data/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    
    # read the configuration file
    if not os.path.exists(filename):
        raise Exception(f"Configuration file not found at {filename}")
    parser.read(filename)

    # check if the section exists in the file
    if parser.has_section(section):
        # get section parameters
        params = parser[section]
        db_config = {
            'host': params['host'],
            'database': params['database'],
            'user': params['user'],
            'password': params['password']
        }
        return db_config
    else:
        raise Exception(f"Section {section} not found in the {filename} file")