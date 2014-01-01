# configuration for building test database
_TestDbConfig = {'CsvFileName' : '../Test.csv', 
                'RawDocTable' : 'raw_doc_test',
                'DocumentsTable' : 'documents_test',
                'TagsTable' : 'tags_test',
                'DocTagsTable' : 'doc_tags_test',
                'FeaturesTable' : 'features', 
                'DocFeaturesTable' : 'docFeatures_test',
                'TagSpecificFeatureTable' : 'tag_specific_features'};
# configuration for building train database
_TrainDbConfig = {'CsvFileName' : '../Train(2)/Train.csv', 
                'RawDocTable' : 'raw_doc_train',
                'DocumentsTable' : 'docs_train',
                'TagsTable' : 'tags_train',
                'DocTagsTable' : 'doc_tags_train',
                'FeaturesTable' : 'features',
                'DocFeaturesTable' : 'docFeatures_train',
                'TagSpecificFeatureTable' : 'tag_specific_features'};
# classifier configuration
ClassifierConfig = {};
# working feature database configuration for building feature set
DbBuildConfig = {'train' : _TrainDbConfig, 'test' : _TestDbConfig};
ClassifierTableConfig = {'predictedTrain' : 'predicted_features_train',
                         'predictedTest' : 'predicted_features_test'};
DatabaseName = "kaggle_fb_test";
