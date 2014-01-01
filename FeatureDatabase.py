# (c) Evgeny Shavlyugin, 2013
# Wrapper for features tables in database. 
# In built form provides access to
# 1. Features associated with a given tag
# 2. All features used for classification
# 3. Association of features with documents.
# Also this class provides methods to build a features database.

import itertools;
from FeatureExtractor import FeatureExtractor;
from collections import defaultdict;
import md5;
from collections import Counter;

class FeatureDatabase:
	_connection = None; # connection to persistent database (postgresql)
        _memDb = None; # connection to memory database (redis)
        _cursor = None;
        _documentsDatabase = None;
        _featureTable = "";
        _featureDocTable = "";
        _featureDict = set([]);
        _workingDir = "";
        _tagSpecificFeaturesTable = "";
        _featureCntHash = {};
        _cachedDocCount = -1;

	def __init__(self, sqlConnection, memDb, documentsDatabase, 
                     featureTable, featureDocTable, tagSpecificFeaturesTable):
		self._connection = sqlConnection;
                self._memDb = memDb;
                self._cursor = sqlConnection.cursor();
                self._documentsDatabase = documentsDatabase;
                self._featureTable = featureTable;
                self._featureDocTable = featureDocTable;
                self._tagSpecificFeaturesTable = tagSpecificFeaturesTable;
                self._cachedDocCount = -1;
                self._featureCntHash = {};

        def getMaxDocumentId(self):
                if self._cachedDocCount < 0:
                        self._cachedDocCount = self._documentsDatabase.getDocumentCount();
                return self._cachedDocCount;

        def cleanMemTable(self):
                self._memDb.zremrangebyscore('features', '-inf', 'inf');

        # add features with given name to database
        def addFeaturesToMemDictionary(self, features):
                if not features:
                        return;
                features = [md5.md5(f).hexdigest() for f in features];
                pipe = self._memDb.pipeline();
                for feature in features:
                        pipe.zincrby( 'features', feature, 1 );
                pipe.execute();

        # returns md5 of features representing tag with a given name
        def getTagSpecificFeatures(self, tagName):
                c = self._cursor;
                query = "SELECT feature_crc FROM %(tagFeatures)s WHERE tag_crc = b\'%(tagCrc)s\'";
                query = query % { 'tagFeatures' : self._tagSpecificFeaturesTable, 
                                  'tagCrc' : "{0:0128b}".format(int(md5.md5(tagName).hexdigest(),16)) };
                c.execute(query);
                return ["{0:032x}".format(int(x[0], 2)) for x in c.fetchall()];

        def getDocumentsWithFeature(self, featureHashHex, first, count):
                c = self._cursor;
                query = """SELECT doc_id, count FROM %(docFeatures)s WHERE feature_id = b\'%(featureHash)s\'
                           AND doc_id >= %(first)d AND doc_id < %(docLimit)d ORDER BY doc_id"""

                query = query % {'docFeatures' : self._featureDocTable,
                                 'featureHash' : "{0:0128b}".format(int(featureHashHex, 16)),
                                 'docLimit' : first + count,
                                 'first' : first};
                c.execute(query);
                return [(x[0], 1) for x in c.fetchall()];

        # returns feature names in human-readable form
        def getFeatureNamesByMd5Hash(self, featureHashes):
                c = self._cursor;
                res = [];
                for h in featureHashes:
                        query = """SELECT display_name FROM %s WHERE name_crc = b\'%s\'""";
                        query = query % (self._featureTable, "{0:0128b}".format(int(h, 16)));
                        c.execute(query);
                        res += [(h, c.fetchall()[0])];
                return res;

        def addFeaturesToPermanentDictionary(self, features):
                c = self._cursor;
                if not features:
                        return;
                features = list(set(features));
                values = [("{0:0128b}".format(                               
                        int(md5.md5(x).hexdigest(), 16)), x) for x in features];
                for value in values:
                        query = """INSERT INTO %(FeatureTable)s (name_crc, display_name, count) 
                                   SELECT b\'%(hash)s\', %(fmt)s, 1 WHERE NOT EXISTS 
                                     (SELECT * from %(FeatureTable)s WHERE name_crc = b\'%(hash)s\');
                                   """ % { 'FeatureTable' : self._featureTable, 'hash' : value[0], 
                                           'fmt' : '%s' };
                        params = (value[1]);
                        c.execute(query, params);
                        self._connection.commit();

        def addTagSpecificFeatures(self, tag, features):
                c = self._cursor;
                if not features:
                        return;
                for feature in features:
                        query = """INSERT INTO %(DocSpecFeatures)s (tag_crc, feature_crc) VALUES
                                   (b\'%(tag_crc)s\', b\'%(feature_crc)s\')""";
                        query = query % {'DocSpecFeatures' : self._tagSpecificFeaturesTable,
                                         'tag_crc' : "{0:0128b}".format(
                                                 int(md5.md5(tag).hexdigest(), 16)), 
                                         'feature_crc' : "{0:0128b}".format(
                                                 int(md5.md5(feature).hexdigest(), 16))};
                        c.execute(query);
                self._connection.commit();

        def _loadFeatureDict(self):
                if len(self._featureDict) != 0:
                        return;
                c = self._cursor;
                c.execute( """SELECT name_crc FROM %s""" % self._featureTable );
                items = c.fetchall();
                self._featureDict |= set(["{0:032x}".format(int(item[0], 2)) for item in items]);
                print "feature dictionary size = ", len(self._featureDict);
                print self._featureDict;

        def _addFeaturesToDocFeatureTable(self, docName, features):
                c = self._cursor;
                self._loadFeatureDict();
                features = [(f,cnt) for (f,cnt) in features if md5.md5(f).hexdigest() in self._featureDict];
                if not features:
                        return;
                values = ','.join( map( lambda (feature, count): "(%d, b\'%s\', %d)"
                                        % (docName, 
                                           "{0:0128b}".format(int(md5.md5(feature).hexdigest(), 16)),
                                           count), 
                                        features ) );
                query = """INSERT INTO %(FeatureTable)s (doc_id, feature_id, count) VALUES %(Values)s;
                        """ % { 'FeatureTable' : self._featureDocTable, 'Values' : values };
                c.execute(query);
                self._connection.commit();

        def getFeaturesCount(self, features):
                c = self._cursor;
                scores = [self._memDb.zscore('features', md5.md5(f).hexdigest()) for f in features];
                return [(f, s if s else 0) for (f,s) in zip(features, scores)];

        # determines features that distinguish given tag. It's especailly useful 
        # rare tags as the main algorithm can skip those words.
        # return tuples with feature name and estimated F1 score
        def getFeaturesForTag(self, tag, featureExtractor, docFeaturesRatio, maxFeatureCount):
                docIds = self._documentsDatabase.getDocumentsWithTag(tag, 700);
                documents = self._documentsDatabase.getDocumentsContent(docIds);
                counter = Counter();
                for (id, heading, content, tags) in documents:
                        features = [feature for (feature, count) in
                                    featureExtractor.processText(id, heading, content)];
                        counter.update(features);
                precision = map( lambda (name, count): (name, 1.0 * count / len(docIds)), 
                                 counter.most_common() );
                # We roughly estimate total number of features as cnt * docCount / totalDocCount
                # if the feature is present in sql database. This gives a good estimation 
                # for frequent features. And predefined lower bound 400 for the rest of the features. 
                featureListTag = counter.most_common();
                featureListTotal = self.getFeaturesCount([name for (name, count) in counter.most_common()]);
                featureCountTotal = map( lambda (name, count): (name, 400 if count == 0 
                                                                else count * docFeaturesRatio), 
                                         featureListTotal );
                tagsCount = self._documentsDatabase.getTagCount( tag );
                featureTagCount = map( lambda (name, count): 
                                       (name, 1.0 * count * tagsCount / len(docIds)), featureListTag );
                featureTagMap = {};
                for (name, count) in featureTagCount:
                        featureTagMap[name] = count;
                recall = [(name, 1.0 * featureTagMap[name] / count) for (name,count) in featureCountTotal];
                res = self._getHighestF1ScoreFeatures( precision, recall, maxFeatureCount );
                return res;

        def buildIndexes( self ):
                c = self._cursor;
                c.execute( "CREATE INDEX %(table)s_index on %(table)s USING BTREE(feature_id, doc_id);" %
                           {'table' : self._featureDocTable} );
                c.execute( "CLUSTER %(table)s USING %(table)s_index;" % {'table' : self._featureDocTable});
                self._connection.commit();
                c.execute( "CREATE INDEX %(table)s_index ON %(table)s USING BTREE(tag_crc);" %
                           {'table' : self._tagSpecificFeaturesTable} );

        def _getHighestF1ScoreFeatures( self, precision, recall, maxFeatureCount ):
                recallMap = {};
                for (name, val) in recall:
                        recallMap[name] = val;
                f1 = map( lambda (name, val): (name, 2.0 * val * recallMap[name] / 
                                               (val + recallMap[name])), precision );
                resF1 = sorted( f1, key = lambda (name, f1_val): f1_val, reverse = True );
                return resF1[0:maxFeatureCount];

        # Build initial feature dictionary based on predefined number of documents
        # Selects the most frequent features in the 
        def buildInitialDictionary(self, featureExtractor, docIds):
                docs = self._documentsDatabase.getDocumentsContent(docIds);
                for (id, heading, content, tags) in docs:
                        features = [feature for (feature, count) in 
                                    featureExtractor.processText(id, heading, content)];
                        self.addFeaturesToMemDictionary(features);
                        self._cleanupIfRequired();

        def collectFeaturesFromDocuments( self, featureExtractor, docIds ):
                docs = self._documentsDatabase.getDocumentsContent(docIds);
                for(id, heading, content, tags) in docs:
                        features = featureExtractor.processText(id, heading, content);
                        self._addFeaturesToDocFeatureTable(id, features);

        def _cleanupIfRequired(self):
                count = self._memDb.zcard('features');
                if count > 5000000:
                        self.cleanupFeatures();

        def cleanupFeatures(self):
                self._memDb.zremrangebyscore('features', '-inf', 7);

        def resetFeaturesTable(self):
                c = self._cursor;
                c.execute( "DROP TABLE IF EXISTS %s;" % self._featureTable );
                c.execute( """CREATE TABLE %(FeatureName)s (
                                  name_crc BIT(128) NOT NULL,
                                  display_name VARCHAR(256) NOT NULL,
                                  count INT NOT NULL,
                                  PRIMARY KEY(name_crc)
                               );
                           """ % { 'FeatureName' : self._featureTable } );
                c.execute( "DROP TABLE IF EXISTS %s;" % self._tagSpecificFeaturesTable );
                c.execute( """CREATE TABLE %(TagSpecificFeatures)s (
                                 id SERIAL,
                                 tag_crc BIT(128) NOT NULL,
                                 feature_crc BIT(128) NOT NULL,
                                 PRIMARY KEY(id)
                              )""" % {'TagSpecificFeatures' : self._tagSpecificFeaturesTable} );
                self._connection.commit();

        def resetFeatureDocTable(self):
                c = self._cursor;
                c.execute( "DROP TABLE IF EXISTS %s;" % self._featureDocTable );
                c.execute( """CREATE TABLE %(FeatureDoc)s (
                                  id BIGSERIAL,
                                  doc_id INT NOT NULL,
                                  feature_id bit(128) NOT NULL,
                                  count INT NOT NULL,
                                  PRIMARY KEY(id)
                              );
                           """ % { 'FeatureDoc' : self._featureDocTable } );
                self._connection.commit();

