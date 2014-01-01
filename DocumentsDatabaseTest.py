# (c) Evgeny Shavlyugin, 2013
# Test of DocumentsStorage class

from DocumentsDatabase import DocumentsDatabase;
import csv;
import operator;
from pyPgSQL import PgSQL;
import redis;
import cProfile;
import sys;

fileName = "../Train(2)/Train.csv";

def run():
        connection = PgSQL.connect(user = "postgres", database = "kaggle_fb_v2" );
        db = DocumentsDatabase(connection, "doc_tags", "raw_documents_train");
        memDb = redis.Redis();
        print db.getTagCount('php');
        print db.getDocumentsWithTag('php', 1000);
#        tagList = db.getTagsList();
 #       print tagList[0][0];
  #      documents = db.getDocumentsWithTag('combinatorics');
   #     print documents;
    #    content = db.getDocumentsContent(documents[0:100]);
     #   for (id, content, header, tags) in content:
      #          print content;
       #         print header;
run();
