# (c) Evgeny Shavlyugin, 2013
# Test of FeaturesDatabase class

from DocumentsDatabase import DocumentsDatabase;
from FeatureDatabase import FeatureDatabase;
from FeatureExtractor import FeatureExtractor;
from ClassifierConfig import DbBuildConfig, DatabaseName;
import csv;
import operator;
from pyPgSQL import PgSQL;
import redis;
import cProfile;
import sys;

fileName = "../Train(2)/Train.csv";

def run():
        connection = PgSQL.connect(user = "postgres", database = DatabaseName);
        memDb = redis.Redis();
        WorkingDbConfig = DbBuildConfig['train'];
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
        f1 = 'ident(gl)';
        f2 = 'header([\'c\', \'++\'])';
        f3 = 'ident(read)';
        f4 = 'non-existing feature';
        processedDocsRatio = 6.0;
        featuresToReturn = 100;
        print featureDb.getFeaturesCount( [f1, f2, f3, f4] );
        print featureDb.getFeaturesForTag( 'sql', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'tsql', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'sql-server', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'sql-server-2008', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'sql-server-2008-r2', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'algorithm', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'php', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'javascript', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'java', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'mfc', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( '64bit', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'actionscript', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'arabic', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'actionscript-3', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'nikon', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'lens', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'linear-algebra', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'combinatorics', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'group-theory', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'statistics', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'glut', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'glfw', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'glew', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'c#', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( '.net-2.0', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'ios6', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'opengl', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'opengl-3', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'version', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'unix', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'osx', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'c++', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'c', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'ruby', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'ruby-on-rails', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'r', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'matlab', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'tex', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'string', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'windows', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'html', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'multithreading', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'c++11', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'image', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'visual-studio-2010', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'latex', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'virtual-memory', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'cron', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'chess', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'freebsd', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'qt', extractor, processedDocsRatio, featuresToReturn );
        print '------------------------------------------'
        print featureDb.getFeaturesForTag( 'qt4', extractor, processedDocsRatio, featuresToReturn );
#run();

cProfile.run('run()');
