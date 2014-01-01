# (c) Evgeny Shavlyugin, 2013
# Test of FeatureExtractor class

from DocumentsDatabase import DocumentsDatabase;
from FeatureExtractor import FeatureExtractor;
from pyPgSQL import PgSQL;
from ClassifierConfig import WorkingDbConfig, DatabaseName;
import cProfile;
import sys;

fileName = "../Train(2)/Train.csv";

def run():
        connection = PgSQL.connect(user = "postgres", database = ClassifierConfig.DatabaseName );
        db = DocumentsDatabase(connection, 
                               WorkingDbConfig['DocTagsTable'], 
                               WorkingDbConfig['RawDocTable'], 
                               WorkingDbConfig['TagsTable'], 
                               WorkingDbConfig['DocumentsTable'] );
        extractor = FeatureExtractor(True);
        docIds = [1,2,3,4,5];
# TODO: documents that breaks parser
 #       docIds = [247, 1070198, 619547];
#        docIds = [247,145698,42027];
        docs = db.getDocumentsContent(docIds);
        for (id, header, content, tags) in docs:
                print extractor.processText(id, header, content);

run();
