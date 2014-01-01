# (c) 2013, Evgeny Shavlyugin
# Implementation of worker threads for a different stages for
# building feature database.

from FeatureExtractor import FeatureExtractor;
from FeatureDatabase import FeatureDatabase;
from DocumentsDatabase import DocumentsDatabase;
from ClassifierConfig import DbBuildConfig, DatabaseName;
from pyPgSQL import PgSQL;
import redis;
import sys;
import time;

class ParallelFeatureDatabaseBuilder:
    _featureDatabase = None;
    _documentsDatabase = None;
    _featureExtractor = None;

    def __init__(self, featureDatabase, documentsDatabase, featureExtractor):
        self._featureDatabase = featureDatabase;
        self._documentsDatabase = documentsDatabase;
        self._featureExtractor = featureExtractor;

    def runInitialDictionaryBuild( self, count, id ):
        totalDocCount = 500000; # this is approximately 0.125 from number of documents in training set 
        bulkSize = 2000;
        for i in range( 0, totalDocCount, bulkSize ):
            ids = self._documentsDatabase.getDocumentIds( i, bulkSize );
            ids = ids[id:len(ids):count];
            print "Processing initdict", i
            if ids:
                self._featureDatabase.buildInitialDictionary( self._featureExtractor, ids );
        
    def runParallelFeatureBuild( self, count, id ):
        totalDocCount = self._documentsDatabase.getDocumentCount();
        for i in range( 0, totalDocCount, 6000 ):
            print "i = ", i;
            ids = self._documentsDatabase.getDocumentIds( i, 6000 );
            ids = ids[id:len(ids):count];
            self._featureDatabase.collectFeaturesFromDocuments( self._featureExtractor, ids );
        
    def runTagsDictionaryBuild( self, procCount, id ):
        tags = self._documentsDatabase.getTagsList();
        features = [];
        counter = 0;
        for tag in tags:
            counter = counter + 1;
            if counter % procCount != id:
                continue;
            tagCount = self._documentsDatabase.getTagCount(tag);
            if tagCount <= 23:
                continue;
            print "Processing ", tag, counter;
            flist = self._featureDatabase.getFeaturesForTag( tag, self._featureExtractor, 1.0, 75 );
	    
            self._featureDatabase.addTagSpecificFeatures( tag, [name for (name, f1) in flist] );
            features = features + [name for (name, f1) in flist];
        if len(features) > 0:
            self._featureDatabase.addFeaturesToPermanentDictionary( features );
            features = [];


connection = PgSQL.connect(user = "postgres", database = DatabaseName);
memDb = redis.Redis( host='localhost', port=6379 );
WorkingDbConfig = DbBuildConfig[sys.argv[1]];

db = DocumentsDatabase(connection, 
                       WorkingDbConfig['DocTagsTable'], 
                       WorkingDbConfig['RawDocTable'], 
                       WorkingDbConfig['TagsTable'], 
                       WorkingDbConfig['DocumentsTable'] );
featureDb = FeatureDatabase(connection, 
                            memDb, 
                            db, 
                            WorkingDbConfig['FeaturesTable'], 
                            WorkingDbConfig['DocFeaturesTable'],
                            WorkingDbConfig['TagSpecificFeatureTable']);

extractor = FeatureExtractor(False);

mode = sys.argv[2];

procCount = int(sys.argv[3]);
procId = int(sys.argv[4]);

if mode.lower() == 'initdict':
    if procId == 0:
        featureDb.cleanMemTable();
    builder = ParallelFeatureDatabaseBuilder( featureDb, db, extractor );
    builder.runInitialDictionaryBuild( procCount, procId );
elif mode.lower() == 'tagfeatures':
    if procId == 0:
        featureDb.resetFeaturesTable();
    builder = ParallelFeatureDatabaseBuilder( featureDb, db, extractor );
    builder.runTagsDictionaryBuild( procCount, procId );
elif mode.lower() == 'allfeatures':
    if procId == 0:
        featureDb.resetFeatureDocTable();
    builder = ParallelFeatureDatabaseBuilder( featureDb, db, extractor );
    builder.runParallelFeatureBuild( procCount, procId );
elif mode.lower() == 'cleanup':
    if procId == 0:
        featureDb.cleanupFeatures();
elif mode.lower() == 'index':
    if procId == 0:
        featureDb.buildIndexes();
