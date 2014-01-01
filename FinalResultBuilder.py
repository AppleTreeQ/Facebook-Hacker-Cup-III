# (c) Evgeny Shavlyugin, 2013
# Class for building final tags set

from DocumentsDatabase import DocumentsDatabase;
from ClassifierConfig import DbBuildConfig, DatabaseName;
import csv;
import operator;
import md5;
import redis;
from pyPgSQL import PgSQL;
import cProfile;
import sys;

def run():
        connection = PgSQL.connect(user = "postgres", database = DatabaseName);
        memDb = redis.Redis( host='localhost', port=6379 );
        WorkingDbConfig = DbBuildConfig['train'];
        db = DocumentsDatabase(connection, 
                               WorkingDbConfig['DocTagsTable'], 
                               WorkingDbConfig['RawDocTable'], 
                               WorkingDbConfig['TagsTable'], 
                               WorkingDbConfig['DocumentsTable'] );

        fileTrain = DbBuildConfig['train']['CsvFileName'];
        fileTest = DbBuildConfig['test']['CsvFileName'];
        TagsColumnIndex = 3;
        HeaderColumnIndex = 1;
        BodyColumnIndex = 2;
        DocNameColumnIndex = 0;
        hashes = {};

        tags = db.getTagsList();
        tagMap = {};
        for tag in tags:
                tagMap[md5.md5(tag).hexdigest()] = tag;

        lineInd = 0;
        with open(fileTrain, "rb") as csvReader:
                for _line in csv.reader(csvReader):
                        line = _line;
                        while len(line) < 4:
                                line = line + ["NULL"];
                        lineInd = lineInd + 1;
                        if lineInd == 1:
                                continue;

                        h = md5.md5( line[HeaderColumnIndex] + line[BodyColumnIndex] ).hexdigest();
                        hashes[h] = line[TagsColumnIndex];

        lineInd = 0;
        c1 = 0;
        c = connection.cursor();
        c.execute("SELECT doc, tag_crc FROM predicted_features_train_new");
        docList = c.fetchall();
        predMap = {};
        totalCountPred = len(docList);
        for (doc, _tag) in docList:
                if not doc in predMap:
                        predMap[doc] = [];
                tag = "{0:032x}".format(int(_tag, 2));
                predMap[doc] += [tagMap[tag]];
        docList = None;
        c.execute( "SELECT name, id FROM raw_doc_test" );
        names = c.fetchall();
        nameMap = {};
        for (name, id) in names:
                nameMap[name] = id;
        totalCount = 0;
        print "Id,Tags";
        with open(fileTest, "rb") as csvReader:
                for _line in csv.reader(csvReader):
                        line = _line;
                        while len(line) < 4:
                                line = line + ["NULL"];
                        lineInd = lineInd + 1;
                        if lineInd == 1:
                                continue;

                        h = md5.md5( line[HeaderColumnIndex] + line[BodyColumnIndex] ).hexdigest();
                        if h in hashes:
                                print "%s,\"%s\"" % (line[DocNameColumnIndex],hashes[h]);
                                c1 = c1+1;
                        else:
                                lst = predMap.get(nameMap[line[DocNameColumnIndex]], []);
                                totalCount += len(lst);
                                print "%s,\"%s\"" % (line[DocNameColumnIndex]," ".join(lst));
                                hashes[h] = " ".join(lst);
run();
