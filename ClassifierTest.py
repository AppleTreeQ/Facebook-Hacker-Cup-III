from ClassifierConfig import DbBuildConfig, ClassifierTableConfig, DatabaseName;
from Classifier import Classifier;
from DocumentsDatabase import DocumentsDatabase;
from FeatureDatabase import FeatureDatabase;
from pyPgSQL import PgSQL;
import md5;
import redis;

def run():
    connection = PgSQL.connect(user = "postgres", database = DatabaseName);
    memDb = redis.Redis( host='localhost', port=6379 );
    TrainDbConfig = DbBuildConfig['train'];
    TestDbConfig = DbBuildConfig['test'];
    trainDocDb = DocumentsDatabase(connection, 
                                   TrainDbConfig['DocTagsTable'], 
                                   TrainDbConfig['RawDocTable'], 
                                   TrainDbConfig['TagsTable'], 
                                   TrainDbConfig['DocumentsTable'] );
    trainFeatureDb = FeatureDatabase(connection, 
                                     memDb, 
                                     trainDocDb, 
                                     TrainDbConfig['FeaturesTable'], 
                                     TrainDbConfig['DocFeaturesTable'],
                                     TrainDbConfig['TagSpecificFeatureTable']);
    testFeatureDb = FeatureDatabase(connection, 
                                    memDb, 
                                    None, 
                                    TestDbConfig['FeaturesTable'], 
                                    TestDbConfig['DocFeaturesTable'],
                                    TestDbConfig['TagSpecificFeatureTable']);

    classifier = Classifier(connection, trainFeatureDb, testFeatureDb, 
                   ClassifierTableConfig['predictedTrain'],
                   ClassifierTableConfig['predictedTest'], trainDocDb);

#    classifier.createTables();
    classifier.createTagPredictTables();
    classifier.cleanClassificationTables();
    tags = trainDocDb.getTagsList();
    s1 = 0;
    s2 = 0;
    for tag in tags:
        features = trainFeatureDb.getTagSpecificFeatures( tag );
        testTag = tag;
        hashes = trainFeatureDb.getTagSpecificFeatures(testTag);
        if not hashes:
            continue;
        c1 = trainDocDb.getTagCount(testTag);
        if c1 <= 25:
            continue;
        s1 += c1;
        print classifier.predictForTag( tag );
    classifier.saveClassificationResults();

run();
