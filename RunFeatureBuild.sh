#!/bin/sh

python RawDatabaseBuilder.py
python BuildDocumentsDatabase.py 'train'
./InitDictBuild.sh 'train' initdict
./InitDictBuild.sh 'train' cleanup
./InitDictBuild.sh 'train' tagfeatures
./InitDictBuild.sh 'train' allfeatures
./InitDictBuild.sh 'train' index
python BuildDocumentsDatabase.py 'test'
./InitDictBuild.sh 'test' allfeatures
./InitDictBuild.sh 'test' index
./ClassifierP.sh
