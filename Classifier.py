# (c) Evgeny Shavlyugin, 2013
# Class for tags classification. For each tag the decision tree classifier is
# trained. The documents for classification are selected from a set of documents
# that have at least one feature specific for a given tag. 
# The scikit learn Descision tree classifier was used for a classification.  
# This selection is based on inverted index build on document-feature table
# building step. Classification algorithm is run separately for each tag.
# This allow parallelization by running classification for different tags in
# separate threads. See ParallelClassifier.py for details.

import md5;
from collections import Counter;
from sklearn import tree;

class Classifier:
        _cursor = None;
        _connection = None;
        _train_db = None;
        _test_db = None;
        _train_doc_db = None;
        _test_predicted_tags = "";
        _train_predicted_tags = "";
        _new_test_predicted_tags = "";
        _new_train_predicted_tags = "";

	def __init__(self, connection, train_db, test_db, test_predicted_tags, 
                     train_predicted_tags, train_doc_db):
		self._connection = connection;
                self._cursor = connection.cursor();
                self._train_db = train_db;
                self._test_db = test_db;
                self._train_doc_db = train_doc_db;
                self._test_predicted_tags = test_predicted_tags;
                self._train_predicted_tags = train_predicted_tags;
                self._new_test_predicted_tags = test_predicted_tags + "_NEW";
                self._new_train_predicted_tags = train_predicted_tags + "_NEW";

        def createTagPredictTables(self):
                c = self._cursor;
                c.execute("DROP TABLE IF EXISTS %s" % self._new_test_predicted_tags );
                c.execute("DROP TABLE IF EXISTS %s" % self._new_train_predicted_tags );
                self._connection.commit();
                c.execute("CREATE TABLE %s (LIKE %s)" % 
                          (self._new_test_predicted_tags, self._test_predicted_tags) );
                c.execute("CREATE TABLE %s (LIKE %s)" % 
                          (self._new_train_predicted_tags, self._train_predicted_tags) );

        def _getDocumentsCollection( self, tagName, featureDb, first, count ):
                features = featureDb.getTagSpecificFeatures( tagName );
                documents = set([]);
                for feature in features:
                        documents |= set( [x[0] for x in featureDb.getDocumentsWithFeature( 
                                feature, first, count ) ] );
                return list(documents);

        def _runClassification( self, tagName ):
                # python set tends to slow down on a large data set. 
                # Especially for a frequent tags like php. 
		docList = self._getDocumentsCollection( tagName, self._train_db, 0, self._train_db.getMaxDocumentId() + 1 );
                if not docList:
                        return;
                classifier = self._trainClassifier( docList, tagName );
                self._predictLabels(classifier, tagName, self._train_db, self._new_train_predicted_tags);
                self._predictLabels(classifier, tagName, self._test_db, self._new_test_predicted_tags);

        def _collectTagLabels( self, docFeatures, tagName ):
                docList = [doc for (doc, x) in docFeatures.iteritems()];
                docsWithTag = set( self._train_doc_db.getDocumentsWithTag( tagName, 1000000 ) );
                return [(1 if doc in docsWithTag else -1) for doc in docList];

        def _trainClassifier( self, trainDocSet, tagName ):
                # TODO: tune minimal features parameter
                print "Number of entries: ", len(trainDocSet);
                numDocs = min(5000, len(trainDocSet) / 4);
                print "Training classifier for ", tagName;
                # train parameters
                trainSet = trainDocSet[0:numDocs];
                matr = self._collectFeatures( trainSet, tagName, self._train_db,
                                              self._train_predicted_tags );
                labels = self._collectTagLabels( matr, tagName );
                # cross validation parameters
                cvSet = trainDocSet[3*numDocs:4*numDocs];
                cvMatr = self._collectFeatures( cvSet, tagName, self._train_db,
                                                self._train_predicted_tags );
                cvLabels = self._collectTagLabels( matr, tagName );
                resClassifier = None;
                resAccuracy = 0.0;
                for depth in range(1,12):
                        for leafSize in range(2,8):
                                classifier = self._trainSkDecisionTreeClassifier( matr, labels, depth, leafSize );
                                predictions = classifier.predict( [x for (doc, x) in cvMatr.iteritems()] );
                                accuracy = 1.0 * sum([abs(a-b) for (a,b) in zip(predictions, cvLabels)]) / len(cvLabels);
                                if accuracy > resAccuracy:
                                        resAccuracy = accuracy;
                                        resClassifier = classifier;
                print "Classifier for ", tagName, " is trained";
                print "tag ", tagName, ",accuracy", resAccuracy;
                return resClassifier;

        def cleanClassificationTables(self):
                c = self._cursor;
                c.execute( "DELETE FROM %s" % self._new_test_predicted_tags );
                c.execute( "DELETE FROM %s" % self._new_train_predicted_tags );
                self._connection.commit();

        def saveClassificationResults(self):
                c = self._cursor;
                query1 = "DELETE FROM %s";
                query2 = "INSERT INTO %s SELECT * FROM %s";
                c.execute( query1 % (self._test_predicted_tags) );
                c.execute( query2 % (self._test_predicted_tags, self._new_test_predicted_tags) );
                c.execute( query1 % (self._train_predicted_tags) );
                c.execute( query2 % (self._train_predicted_tags, self._new_train_predicted_tags) );
                self._connection.commit();

        def _trainSkDecisionTreeClassifier(self, featuresMatr, labels, mdepth, splitSize):
                clf = tree.DecisionTreeRegressor(min_samples_split=splitSize, max_depth=mdepth);
                X = [x for (doc, x) in featuresMatr.iteritems()];
                clf = clf.fit( X, labels );
                return clf;

        def _predictLabels(self, classifier, tagName, featureDb, predictedTableName):
                bulkSize = 1200000;
                # for frequent tags in order to increase performance the data is processing by parts.
                upperBound = featureDb.getMaxDocumentId() + 1;
                for i in range(0, upperBound, bulkSize):
                        print "predicting for %s from %d to %d" % (tagName, i, i + bulkSize);
                        documents = self._getDocumentsCollection(tagName, featureDb, i, i + bulkSize);
                        self._predictLabelsForPart( documents, classifier,
                                                    tagName, featureDb, predictedTableName );

        def _predictLabelsForPart(self, documents, classifier, tagName, featureDb, predictedTableName):
                matr = self._collectFeatures( documents, tagName, featureDb,
                                              predictedTableName );
                docList = matr.items();
                # predicting tags in all pass produce memory error of skLearn.
                # so we're processing data by parts in few passes.
                bulkSize = 10000;
                predictedLabels = [];
                for i in range( 0, len(docList), bulkSize ):
                        toPredict = [x for (doc, x) in docList[i:i+bulkSize]];
                        temp = classifier.predict( toPredict );
                        predictedLabels += list(temp);
                pos = sum( [1 for x in predictedLabels if x == 1] );
                neg = sum( [1 for x in predictedLabels if x == -1] );
                print "Total = ", len(predictedLabels);
                print "Positive = ", pos;
                print "Negative = ", neg;
                self._saveResults( tagName, [doc for (doc, x) in docList], 
                                   predictedLabels, predictedTableName );
                
        def _saveResults(self, tagName, documents, predictedLabels, tblName):
                c = self._cursor;
                tagCrc = "{0:0128b}".format(int(md5.md5(tagName).hexdigest(), 16));
                for (doc, lbl) in zip(documents, predictedLabels):
                        if lbl == 1:
                                c.execute("INSERT INTO %s(tag_crc, doc) VALUES( b\'%s\', %d )" %
                                          (tblName, tagCrc, doc) );
                        
                self._connection.commit();

        def _classify( self, classifier, docWithFeatures ):
                return None;

        def _getDocumentsLabels(self, tagName, documents, featureDb):
                docList = set(self._train_doc_db.getDocumentsWithTag(tagName));
                return [(1 if doc in docList else 0) for doc in documents];

        def _collectFeatures( self, documents, tagName, featureDb, predTagTable ):
                res = {};
                if not documents:
                        return [];
                minInd = min( documents );
                maxInd = max( documents );
                first = minInd;
                count = maxInd - minInd + 1;
                for doc in documents:
                        res[doc] = [];
                self._collectTagSpecificFeatures(res, tagName, featureDb, first, count);
                # Experiments shows that tags correlation doesn't affect final results at all
                # So we don't using predicted tags features
                # self._collectPredictedTagFeatures(res, tagName, predTagTable, 6 );
                return res;

        def _collectTagSpecificFeatures(self, docFeatureMap, tagName, featureDb, first, count):
                tagSpecFeatures = featureDb.getTagSpecificFeatures( tagName );
                for featureHash in tagSpecFeatures:
                        docSet = {};
                        _docSet = featureDb.getDocumentsWithFeature( featureHash, first, count );
                        for (doc, cnt) in _docSet:
                                docSet[doc] = cnt;
                        for doc in docFeatureMap.keys():
                                docFeatureMap[doc] += [docSet.get(doc, 0)];

        def _collectPredictedTagFeatures(self, docFeatureMap, tagName, predTagTable, maxCount):
                documents = docFeatureMap.keys();
                tags = self._getTagsCoocurences( tagName, self._train_predicted_tags );
                tags = sorted( tags, key = lambda x: -x[1] )[0:maxCount];
                desiredTagList = [tag for (tag, c) in tags];
                for tag in desiredTagList:
                        docSet = set(self._getDocsWithTag( predTagTable, tag ));
                        for doc in docFeatureMap.keys():
                                docFeatureMap[doc] += [1 if doc in docSet else 0];

        def _getDocsWithTag( self, predTagTable, tagCrc ):
                c = self._cursor;
                c.execute( "SELECT doc FROM %s WHERE tag_crc = b\'%s\'" 
                           % (predTagTable, "{0:0128b}".format( int( tagCrc, 16 ) )) );
                return [x[0] for x in c.fetchall()];

        def _getTagsCoocurences( self, tagName, tableName ):
                c = self._cursor;
                tagCrc = "{0:0128b}".format(int(md5.md5(tagName).hexdigest(), 16));
                query = """SELECT tag_crc, COUNT(*) FROM %s WHERE 
                           doc IN (SELECT doc FROM %s WHERE tag_crc = b\'%s\') GROUP BY tag_crc""";
                query = query % (tableName, tableName, tagCrc);
                c.execute(query);
                return [("{0:032x}".format(int(x[0], 2)), x[1]) for x in c.fetchall()];

        def predictForTag(self, tagName):
                print "predicting for %s" % tagName;
                self._runClassification(tagName);

        def savePredictions(self):
                c = self._cursor;
                c.execute( "DELETE from %s" % self._test_predicted_tags );
                c.exeucte( "DELETE from %s" % self._train_predicted_tags );
                c.execute( "INSERT INTO %s SELECT * FROM %s" % 
                           (self._test_predicted_tags, self._new_test_predicted_tags) );
                c.execute( "INSERT INTO %s SELECT * FROM %s" % 
                           (self._train_predicted_tags, self._new_train_predicted_tags) );                
                self._connection.commit();

        def createTables(self):
                c = self._cursor;
                c.execute("DROP TABLE IF EXISTS %s" % self._test_predicted_tags);
                c.execute("DROP TABLE IF EXISTS %s" % self._train_predicted_tags);
                tagTbl = """CREATE TABLE %s (
                              tag_crc bit(128) NOT NULL,
                              doc INT NOT NULL,
                              prob DOUBLE PRECISION NOT NULL,
                              PRIMARY KEY( tag_crc, doc )
                           )""";
                index1Query = "CREATE INDEX %s ON %s USING BTREE(tag_crc)";
                index2Query = "CREATE INDEX %s ON %s USING BTREE(doc)";
                c.execute( tagTbl % self._test_predicted_tags );
                c.execute( index1Query % (self._test_predicted_tags + "_index1", self._test_predicted_tags) );
                c.execute( index2Query % (self._test_predicted_tags + "_index2", self._test_predicted_tags) );
                c.execute( tagTbl % self._train_predicted_tags );
                c.execute( index1Query % (self._train_predicted_tags + "_index1", self._train_predicted_tags) );
                c.execute( index2Query % (self._train_predicted_tags + "_index2", self._train_predicted_tags) );
                self._connection.commit();
