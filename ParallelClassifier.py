# (c) Evgeny Shavlyugin, 2013
# Implementation of a single worker thread for tags classification

from ClassifierConfig import DbBuildConfig, ClassifierTableConfig, DatabaseName;
from Classifier import Classifier;
from DocumentsDatabase import DocumentsDatabase;
from FeatureDatabase import FeatureDatabase;
from pyPgSQL import PgSQL;
import md5;
import redis;
import sys;

def run(procId, procCount):
    connection = PgSQL.connect(user = "postgres", database = DatabaseName);
    memDb = redis.Redis( host='localhost', port=6379 );
    TrainDbConfig = DbBuildConfig['train'];
    TestDbConfig = DbBuildConfig['test'];
    trainDocDb = DocumentsDatabase(connection, 
                                   TrainDbConfig['DocTagsTable'], 
                                   TrainDbConfig['RawDocTable'], 
                                   TrainDbConfig['TagsTable'], 
                                   TrainDbConfig['DocumentsTable'] );
    testDocDb = DocumentsDatabase(connection, 
                                  TestDbConfig['DocTagsTable'], 
                                  TestDbConfig['RawDocTable'], 
                                  TestDbConfig['TagsTable'], 
                                  TestDbConfig['DocumentsTable'] );
    trainFeatureDb = FeatureDatabase(connection, 
                                     memDb, 
                                     trainDocDb, 
                                     TrainDbConfig['FeaturesTable'], 
                                     TrainDbConfig['DocFeaturesTable'],
                                     TrainDbConfig['TagSpecificFeatureTable']);
    testFeatureDb = FeatureDatabase(connection, 
                                    memDb, 
                                    testDocDb, 
                                    TestDbConfig['FeaturesTable'], 
                                    TestDbConfig['DocFeaturesTable'],
                                    TestDbConfig['TagSpecificFeatureTable']);

    classifier = Classifier(connection, trainFeatureDb, testFeatureDb, 
                   ClassifierTableConfig['predictedTrain'],
                   ClassifierTableConfig['predictedTest'], trainDocDb);

#    if procId == 0:
 #       classifier.createTables();
  #      classifier.createTagPredictTables();
   #     classifier.cleanClassificationTables();

    tags = trainDocDb.getTagsList();
    count = 0;
    for tag in tags:
        count = count + 1;
        if count % procCount != procId:
            continue;
        if count < 9000:
            continue;
        print "Processing ", tag, " ", count;
        c1 = trainDocDb.getTagCount(tag);
        if c1 <= 23:
            continue;
        classifier.predictForTag( tag );


procId = int(sys.argv[2]);
procCount = int(sys.argv[1]);
run(procId, procCount);
