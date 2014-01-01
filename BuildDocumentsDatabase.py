# (c) Evgeny Shavlyugin, 2013
# Script for building auxillary tables for raw documents database.
# 1. document-tags relation and indexes for a fast access to documents with a given tag
# 2. document-id table for faster access to document ranges.
# The only parameter to script is configuration name. See ClassifierConfig for details

from DocumentsDatabase import DocumentsDatabase;
from ClassifierConfig import DbBuildConfig, DatabaseName;
import csv;
import operator;
from pyPgSQL import PgSQL;
import cProfile;
import sys;

def run(configName):
        connection = PgSQL.connect(user = "postgres", database = DatabaseName);
        WorkingDbConfig = DbBuildConfig[configName];
        db = DocumentsDatabase(connection, 
                               WorkingDbConfig['DocTagsTable'], 
                               WorkingDbConfig['RawDocTable'], 
                               WorkingDbConfig['TagsTable'], 
                               WorkingDbConfig['DocumentsTable']);
        db.rebuildDatabase();

run(sys.argv[1]);
