'''
Created on Jun 27, 2017

@author: Damian.Dragowski
'''
from six import string_types, text_type
from ovalinfo.com.TermColors import TermColors
from io import open
import csv
import sys
import sqlite3
from random import choice
from string import lowercase
import exceptions


class MBank(object):
    '''
    classdocs
    '''
    __csv_filename = "";
    __db_filename = "";
    __table_name = "";
    __isOk = True;
    __read_mode=0;
    __accountNo = "";
    encoding = 'ISO-8859-2';
    createTableStmt = "";
    connection = 0;
    cursor = 0;
    reader = 0;
    headersLen = 0;

    def __init__(self, csv, db, table, read_mode):
        '''
        Constructor
        '''
        self.__csv_filename=csv;
        self.__db_filename=db;
        self.__table_name=table;
        self.__read_mode=read_mode;
        
    def initDB(self):
        '''
        Initialization of database
        If not exists create new one, if exists only open
        '''
        try:
            self.connection = sqlite3.connect(self.__db_filename);
            self.connection.text_factory = str;
            self.cursor = self.connection.cursor();
        except Exception as e:
            print TermColors.FAIL + "Open database problem";
            print TermColors.FAIL + "Exception %s" % e;
            self.__isOk = False;   
    def closeDB(self):
        '''
        Initialization of database
        If not exists create new one, if exists only open
        '''
        try:
            self.connection.commit();
            self.cursor.close();
        except Exception as e:
            print TermColors.FAIL + "Closing database problem";
            print TermColors.FAIL + "Exception %s" % e;
            self.__isOk = False;              
        
    def openCSV(self):
        '''
        Open csv file
        ''' 
        if self.__isOk:
            if isinstance(self.__csv_filename, string_types):
                print TermColors.BOLD + "Openning file " + self.__csv_filename;
                filehandler = open(self.__csv_filename, mode=self.__read_mode, encoding=self.encoding);
                line_offset = [];
                offset = 0;
                account_no=0;
                counter=0;
                data_no=0;
                for line in filehandler:
                    if line.find("#Numer rachunku;") > -1:
                        account_no = counter+1;
                    if line.find("#Data operacji;") > -1:
                        data_no = counter+1;                        
                    line_offset.append(offset);
                    offset += len(line)+1;
                    counter=counter+1;
                    
                filehandler.seek(line_offset[account_no]);
                self.__accountNo = filehandler.readline();
                filehandler.seek(line_offset[data_no-1]);
                try:
                    dialect = csv.Sniffer().sniff(filehandler.readline());
                except TypeError:
                    dialect = csv.Sniffer().sniff(str(filehandler.readline()));

                filehandler.seek(line_offset[data_no-1]);
                reload(sys);
                sys.setdefaultencoding(self.encoding);

                reader = csv.reader(filehandler, dialect);
                headers = [header.strip() for header in next(reader)];

                cols = [];
        
                for f in headers:
                    if ( len (f) == 0 ):
                        f = "".join(choice(lowercase) for i in range(7));
                    cols.append("\"%s\" %s" % (f.strip('#').decode(self.encoding).encode('utf8'), "TEXT"));
                    self.headersLen += 1; 
                        
                self.createTableStmt = "CREATE TABLE %s (%s)" % (self.__table_name, ",".join(cols));
                
                filehandler.seek(line_offset[data_no]);
                self.reader = csv.reader(filehandler, dialect);

            else: 
                print TermColors.FAIL + "Could not import from pipeline";
                self.__isOk = False;  
   
    def createTable(self):
        '''
        Clear table or create one
        '''
        if self.__isOk:
            try:
                print TermColors.HEADER + " Creating table with statement";
                print TermColors.HEADER + self.createTableStmt;
                self.cursor.execute(self.createTableStmt)
            except sqlite3.DatabaseError as xx: 
                print TermColors.WARNING + "Table Already exists";
                print TermColors.WARNING + "Exception %s" % xx;
                None                
            except Exception as e:
                print TermColors.FAIL + "Could create table";
                print TermColors.FAIL + "Exception %s" % e;
                self.__isOk = False;  
        else:
            print TermColors.FAIL + "Table not created";
    def importData(self):
        '''
        import csv file to database
        '''  
        i = 0;
        for row in self.reader:
            i=i+1;
            if len(row) == 0:
                print TermColors.WARNING + "Empty line in %d" % i; 
                continue;
            if len(row) != self.headersLen:
                print TermColors.WARNING + "incorrect line %d" % i;
                continue;
                
            try:
                stmt = "INSERT INTO %s VALUES (%s)" % (self.__table_name, ','.join(['?']* self.headersLen));
                #row_decoded= [];
                #for r in row:
                #    row_decoded.append( unicode(r, 'utf8'));
                self.cursor.execute(stmt, row);
            except Exception as e:
                print TermColors.FAIL + "Inserting error in line %d" %i;                
                print TermColors.FAIL + "Exception %s" % e;
                
    def isOk(self):
        '''
        import csv file to database
        '''     
        return __isOk;