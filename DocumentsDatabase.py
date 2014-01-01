# (c) Evgeny Shavlyugin 2013
# A set of tables for working with a documents collection.
# If tags are unknown the relation documents-tags are left empty.

import md5;
import itertools;

class DocumentsDatabase:
    _connection = None;
    _counter = 0;
    _cursor = None;
    _docTagsTable = "";
    _rawTableName = "";
    _tagTable = "";
    _docTable = "";

    def __init__(self, connection, docTagsTable, rawTableName, tagTable, docTable):
        self._connection = connection;
        self._cursor = connection.cursor();
        self._docTagsTable = docTagsTable;
        self._tagTable = tagTable;
        self._rawTableName = rawTableName;
        self._docTable = docTable;

    def rebuildDatabase(self):
        self._clean();
        c = self._cursor;
        print "Counting number of documents";
        c.execute("SELECT COUNT(*) FROM %s;" % self._rawTableName);
        totalCount = c.fetchone()[0];
        print "Total documents count = %d" % totalCount;
        print "Db build started...";
        bulkSize = 850011;
        c.execute("INSERT INTO %(DocT)s SELECT id FROM %(RawDocT)s" % {'DocT' : self._docTable,
                                                                        'RawDocT' : self._rawTableName});
        self._connection.commit();
        for i in range(0, totalCount, bulkSize):
            c.execute("""SELECT %(RawDocT)s.id, %(RawDocT)s.name, %(RawDocT)s.tags 
                         FROM %(RawDocT)s JOIN 
                         (SELECT id FROM %(DocT)s WHERE id > %(first)d and id <= %(firstCount)d) subQ
                         ON subQ.id = %(RawDocT)s.id""" % {'RawDocT' : self._rawTableName, 
                                                           'DocT' : self._docTable,
                                                           'first' : i,
                                                           'firstCount' : min(i + bulkSize, totalCount)});
            print "Processing from %d to %d..." % (i, min(bulkSize + i, totalCount));
            ids, names, tags = zip(*c.fetchall());
            bulkSize2 = 2000;
            for j in range(0, bulkSize, bulkSize2):
                self._processPart(ids[j:j+bulkSize2], names[j:j+bulkSize2], tags[j:j+bulkSize2]);
                print 'j=',j;
                if j % 40000 == 0:
                    self._connection.commit();
        c.execute("create index %(DocTags)s_index ON %(DocTags)s USING BTREE(tag_id)" 
                  % {'DocTags' : self._docTagsTable } ); 
        c.execute("cluster %(DocTags)s using %(DocTags)s_index" % {'DocTags' : self._docTagsTable } );
        self._connection.commit();

    def getTagsList(self):
        c = self._cursor;
        c.execute("SELECT name FROM %s" % self._tagTable);
        return [x[0] for x in c.fetchall()];

    def getTagCount(self, tag):
        c = self._cursor;
        print tag;
        c.execute("SELECT count(*) FROM %s WHERE tag_id = b\'%s\'" 
                  % (self._docTagsTable, "{0:0128b}".format(int(md5.md5(tag).hexdigest(), 16) ) ));
        return c.fetchall()[0][0];

    def getDocumentsWithTag(self, tag, limit):
        c = self._cursor;
        query = "SELECT doc_id FROM %(docTagT)s WHERE tag_id = b\'%(hash)s\' ORDER BY doc_id LIMIT %(limit)s";
        query = query % {'docTagT' : self._docTagsTable, 
                         'hash' : "{0:0128b}".format( int(md5.md5(tag).hexdigest(), 16) ),
                         'limit' : str(limit) };
        c.execute(query);
        return map( lambda x : x[0], c.fetchall() );

    def getDocumentCount(self):
        c = self._cursor;
        query = "SELECT COUNT(*) FROM %s" % self._rawTableName;
        c.execute(query);
        return c.fetchone()[0];

    # get document ids from first to first + count
    def getDocumentIds(self, first, count):
        c = self._cursor;
        query = """SELECT id FROM %(docT)s WHERE id > %(offset)d and 
                   id <= %(countOff)d""" % {'docT' : self._rawTableName, 
                                            'offset' : first, 'countOff' : first + count};
        c.execute(query);
        return map( lambda x : x[0], c.fetchall() );

    def getDocumentsContent(self, docIds):
        c = self._cursor;
        query = ','.join([str(id) for id in docIds]);
        c.execute("""SELECT %(rawT)s.id, %(rawT)s.heading, %(rawT)s.content, %(rawT)s.tags 
                     FROM %(rawT)s WHERE %(rawT)s.id IN (%(query)s)""" 
                     % {'rawT' : self._rawTableName, 'query' : query});
        res = c.fetchall();
        return res;

    def _processPart(self, ids, names, tags):
        if len(ids) == 0:
            return;
        c = self._cursor;
        docValues = [];
        for (id, name, tagStr) in zip(ids, names, tags):
            alltags = tagStr.split();
            for tag in alltags:
                query = """INSERT INTO %(table)s (SELECT \'%(tag)s\' WHERE NOT EXISTS 
                (SELECT * FROM %(table)s WHERE name = \'%(tag)s\'))""" % {'table' : self._tagTable, 'tag' : tag};
                c.execute(query);
            alltags = ['b\'%s\'' % '{0:0128b}'.format(int(md5.md5(x).hexdigest(), 16)) for x in alltags];
            docValues += [','.join(["(%s, %s)" % (tag, str(id)) for tag in alltags])];
        c.execute("INSERT INTO %s (tag_id, doc_id) VALUES %s" % (self._docTagsTable, ','.join(docValues) ));

    def _createTables(self):
        c = self._cursor;

        c.execute( """CREATE TABLE %s (
                         name varchar(32),
                         PRIMARY KEY(name)
                      );
                   """ % self._tagTable );
        c.execute( """CREATE TABLE %s (
                   id SERIAL,
                   doc_id INT NOT NULL,
                   tag_id bit(128) NOT NULL,
                   PRIMARY KEY(id) );""" % self._docTagsTable );
        c.execute( """CREATE TABLE %s (
                        id INT NOT NULL,
                        PRIMARY KEY(id)
                      );""" % (self._docTable) );
        self._connection.commit();

    def _clean(self):
        c = self._cursor;
        c.execute( "DROP TABLE IF EXISTS %s;" % self._docTagsTable );
        c.execute( "DROP TABLE IF EXISTS %s;" % self._tagTable );
        c.execute( "DROP TABLE IF EXISTS %s;" % self._docTable );
        self._connection.commit();
        self._createTables();

