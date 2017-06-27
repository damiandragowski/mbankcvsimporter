'''
Created on Apr 3, 2017

@author: Damian.Dragowski
'''
import sys
import argparse

from ovalinfo.com.MBank import MBank
from ovalinfo.com.TermColors import TermColors



if sys.version_info[0] > 2:
    read_mode = 'rt';
else:
    read_mode = 'rU';
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''
Import mbank csv file to sqlite table
''');
    parser.add_argument('csv_file', type=str, help='CSV filename', default=None);
    parser.add_argument('db_file', type=str, help='SQLite database file', default=None);
    parser.add_argument('table_name', type=str, help='table name inside database', default=None);



    args = parser.parse_args();
    convertFile = MBank(args.csv_file, args.db_file, args.table_name,read_mode);
    convertFile.initDB();
    convertFile.openCSV();
    convertFile.createTable();
    convertFile.importData();
    convertFile.closeDB();
    
    if (  convertFile.isOk == True ):
        print TermColors.OKBLUE + "Import to table ended";
