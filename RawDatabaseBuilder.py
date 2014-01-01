# (c) Evgeny Shavlyugin, 2013
# Script for import raw csv data to postgres database

from DocumentsDatabase import DocumentsDatabase;
from ClassifierConfig import DbBuildConfig, DatabaseName;
import csv;
import operator;
import md5;
from pyPgSQL import PgSQL;
import cProfile;
import sys;

def run():
        tableNameTrain = DbBuildConfig['train']['RawDocTable'];
        fileNameTrain = DbBuildConfig['train']['CsvFileName'];
        tableNameTest = DbBuildConfig['test']['RawDocTable'];
        fileNameTest = DbBuildConfig['test']['CsvFileName'];
        TagsColumnIndex = 3;
        HeaderColumnIndex = 1;
        BodyColumnIndex = 2;
        DocNameColumnIndex = 0;
        hashes = set([]);

        connection = PgSQL.connect(user = "postgres", database = DatabaseName );
        c = connection.cursor();
        c.execute("""DROP TABLE IF EXISTS %s;""" % tableNameTrain );
        c.execute("""DROP TABLE IF EXISTS %s;""" % tableNameTest );
        queryStr = """CREATE TABLE %s (
                        id BIGSERIAL,
                        name VARCHAR(20) NOT NULL,
                        heading TEXT,
                        content TEXT,
                        tags TEXT,
                        PRIMARY KEY(id)); """;
        c.execute( queryStr % tableNameTrain );
        c.execute( queryStr % tableNameTest );
        connection.commit();
        lineInd = 0;
	limit = 200000;
        with open(fileNameTrain, "rb") as csvReader:
                for _line in csv.reader(csvReader):
                        line = _line;
                        while len(line) < 4:
                                line = line + ["NULL"];
                        lineInd = lineInd + 1;
                        if lineInd == 1:
                                continue;

                        h = md5.md5( line[HeaderColumnIndex] + line[BodyColumnIndex] ).hexdigest();
                        tags = line[TagsColumnIndex].split();
                        # second clause is a limitation for frequent tags like 'php'
                        if not h in hashes:
                                query = "INSERT INTO %s(name, heading, content, tags)" % tableNameTrain;
                                query = query + " VALUES(%s, %s, %s, %s);"; 
                                c.execute(query, line);
                        hashes |= set([h]);
                        if lineInd == limit:
				break;
			if lineInd % 5000 == 0:
                                print lineInd, ' documents processed';
                                connection.commit();
        lineInd = 0;
        with open(fileNameTest, "rb") as csvReader:
                for _line in csv.reader(csvReader):
                        line = _line;
                        while len(line) < 4:
                                line = line + ["NULL"];
                        lineInd = lineInd + 1;
                        if lineInd == 1:
                                continue;
                        h = md5.md5( line[HeaderColumnIndex] + line[BodyColumnIndex] ).hexdigest();
                        if not h in hashes:
                                query = "INSERT INTO %s(name, heading, content, tags)" % tableNameTest;
                                query = query + " VALUES(%s, %s, %s, %s);"; 
                                c.execute(query, line);
                        hashes |= set([h]);
                        if lineInd == limit:
				break;
			if lineInd % 5000 == 0:
                                print lineInd, ' documents processed';
                                connection.commit();
        lineInd = 0;
        connection.commit();
run();
#cProfile.run('run()');
