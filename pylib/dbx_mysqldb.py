#!/usr/bin/env python
# Copyright (c) 2009-2010 ActiveState Software Inc.
# See the file LICENSE.txt for licensing information.

"""
Code to work with MySQL databases using the
MySQLdb library
"""

import os, sys, re
import logging
from contextlib import contextmanager

log = logging.getLogger("dbx_mysqldb")
#log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)

import dbxlib
try:
    import MySQLdb
    loaded = True
    disabled_reason = None
except ImportError, ex:
    sys.stderr.write("dbx_mysqldb.py: Failed to load MySQL: %s\n" % (ex,))
    log.exception("Failed to load MySQL")
    import missingAdaptor
    MySQLdb = missingAdaptor.MissingAdaptor()
    MySQLdb.adaptorName = 'MySQL'
    loaded = False
    disabled_reason = "Couldn't find database adapter MySQLdb: %s" % (ex,)

#TODO: Lots of this is in common with postgres, so fold it

_unrec_types = {}

# This is the same for all databases and tables:
_int_type_names = ('smallint', 'integer', 'bigint', 'serial', 'bigserial')
_float_type_names = ('decimal', 'numeric', 'real', 'double precision')
_currency_type_names = ('money')


def getSchemaColumnNames():
    return ['column_name', 'data_type', 'is_nullable', 'column_default',
            'character_maximum_length', 'is_primary_key']

def columnTypeIsInteger(typeName):
    return typeName in _int_type_names

def columnTypeIsReal(typeName):
    return typeName in _float_type_names

def columnTypeIsBlob(typeName):
    return typeName == "BLOB"

class Connection(object):
    # Buf 88655: the database name is part of the connection.
    partNames = {'host':'host',
                 'user':'user',
                 'db':'db',
                 'port':'port'}
    def __init__(self, dbname, args):
        #log.debug("Connection: host:%r, socket:%r, port:%r, user:%r, password:%r",
        #          args.get('host', "???"),
        #          args.get('socket', "???"),
        #          args.get('port', "???"),
        #          args.get('username', "???"),
        #          args.get('password', "???"))

        # See koDBConnMySQL.py::_params_from_connection
        self.host = args['host']
        self.socket = args.get('socket', "")
        self.port = args.get('port')
        self.user = args['username']
        self.db = dbname
        self.password = args.get('password')
        self.hasPassword = args.get('hasPassword', False)

    def getConnectionParameters(self):
        """
        Return this has a hash, then use apply to invoke the function
        """
        parts = {}
        for a in self.partNames.keys():
            res = getattr(self, a)
            if res:
                parts[self.partNames[a]] = res
        if self.password is not None:
            parts['passwd'] = self.password
        if 'port' in parts:
            parts['port'] = int(parts['port'])
        val = getattr(self, 'socket', None)
        if val:
            parts['unix_socket'] = val
        return parts

    def getConnectionDisplayValues(self):
        return "%s@%s" % (self.user, self.host)
        
class ColumnInfo(object):
    def __init__(self, name, type, nullable, default_value,
                 max_length, is_primary_key):
        self.column_name = name
        self.name = name   #Synonym, need a better way to manage this
        self.data_type = type
        self.type = type   #Synonym...
        self.is_nullable = nullable
        self.nullable = nullable   #Synonym...
        self.has_default_value = default_value != None
        self.column_default = default_value
        self.default_value = default_value #Synonym...
        if is_primary_key or (is_primary_key == "True"):
            self.is_primary_key = 1
        else:
            self.is_primary_key = 0
        self.character_maximum_length = max_length

        self.prettyName_to_attrName = {
            'nullable?': 'nullable',
            'default value': 'default_value',
            'primary key?': 'is_primary_key',
            }
        
    def __repr__(self):
        return ("<ColumnInfo: name:%r, "
                + "type:%r, "
                + "nullable:%r, \n"
                + "has_default_value:%r "
                + "default_value:%r, "
                + "is_pri_key:%r, "
                + "max_length:%r>") % (
        self.name,
        self.type,
        self.nullable,
        self.has_default_value ,
        self.default_value,
        self.is_primary_key,
        self.character_maximum_length
        )

    def id_from_name(self, prettyName):
        return self.prettyName_to_attrName.get(prettyName, prettyName)

class OperationalError(MySQLdb.OperationalError):
    pass

class DatabaseError(MySQLdb.DatabaseError):
    pass

class Database(dbxlib.CommonDatabase):
    # args should be: host, username=None, password=None, port=None
    handles_prepared_stmts = False
    def __init__(self, args, dbname=None):
        self._dbname = dbname
        self.connection = Connection(dbname, args)
        self._init_db()

    def _init_db(self):
        self.col_info_from_table_name = {}

    def _qualifyTableName(self, table_name):
        # MySQL doesn't use quotes.
        if hasattr(self, '_dbname'):
            return "%s.%s" % (self._dbname, table_name)
        else:
            return table_name

    def getConnectionDisplayInfo(self):
        return self.connection.getConnectionDisplayValues()
        
    @contextmanager
    def connect(self, commit=False, cu=None):
        """ See dbx_sqlite3.py::connect docstring for full story
        @param commit {bool} 
        @param cu {sqlite3.Cursor}
        """
        if cu is not None:
            yield cu
        else:
            params = self.connection.getConnectionParameters()
            try:
                conn = apply(MySQLdb.connect, (), params)
            except:
                log.exception("Failed to connect to mysql, with params:%s", params)
                raise
            cu = conn.cursor()
            try:
                yield cu
            finally:
                if commit:
                    conn.commit()
                cu.close()
                conn.close()

    # get metadata about the database and tables

    def listDatabases(self):
        try:
            query = """select distinct table_schema
                       from information_schema.tables
                       where table_type = 'BASE TABLE'"""
            with self.connect() as cu:
                cu.execute(query)
                names = [row[0] for row in cu.fetchall()]
                return names
        except MySQLdb.OperationalError, ex:
            raise OperationalError(ex)
        except MySQLdb.DatabaseError, ex:
            raise DatabaseError(ex)
                
    def listAllTablePartsByType(self, typeName):
        try:
            query = """select table_name
                       from information_schema.tables
                       where table_type = '%s'
                         and table_schema = '%s' """ % (typeName,
                                                        self._dbname)
            with self.connect() as cu:
                cu.execute(query)
                names = [row[0] for row in cu.fetchall()]
                return names
        except MySQLdb.OperationalError, ex:
            raise OperationalError(ex)
        except MySQLdb.DatabaseError, ex:
            raise DatabaseError(ex)
        
    def listAllTableNames(self, dbname):
        try:
            query = """select table_name
                       from information_schema.tables
                       where table_type = '%s'
                         and table_schema = '%s' """ % ('BASE TABLE',
                                                        dbname)
            with self.connect() as cu:
                cu.execute(query)
                names = [row[0] for row in cu.fetchall()]
                return names
        except MySQLdb.OperationalError, ex:
            raise OperationalError(ex)
        except MySQLdb.DatabaseError, ex:
            raise DatabaseError(ex)
        
    def listAllColumnNames(self, dbname, table_name):
        try:
            query = ("select column_name from information_schema.columns "
                     + "where table_schema = '%s' "
                     + " and table_name = '%s'") % (dbname, table_name)
            with self.connect() as cu:
                cu.execute(query)
                names = [row[0] for row in cu.fetchall()]
                return names
        except MySQLdb.OperationalError, ex:
            raise OperationalError(ex)
        except MySQLdb.DatabaseError, ex:
            raise DatabaseError(ex)

    def listAllIndexNames(self):
        return self.listAllTablePartsByType('INDEX') #TODO: Verify this
    
    def listAllTriggerNames(self):
        return self.listAllTablePartsByType('TRIGGER') # TODO: Verify this

    #TODO: Add views
    
    def _save_table_info(self, table_name):
        if ';' in table_name:
            raise Exception("Unsafe table_name: %s" % (table_name,))
        import pprint
        if table_name in self.col_info_from_table_name:
            log.debug("_save_table_info: #1 returning %s", pprint.pformat(self.col_info_from_table_name[table_name]))
            return self.col_info_from_table_name[table_name]
        # First determine which columns are indexed
        indexed_columns = {}
        index_query = ("select column_name "
                       + "from information_schema.columns "
                       + "where table_name='%s' and table_schema = '%s' "
                       + " and column_key='PRI'") % (table_name, self._dbname)
        main_query = ("select column_name, data_type, is_nullable, "
                      + " column_default, character_maximum_length "
                      + "from information_schema.columns "
                      + "where table_name='%s' and table_schema = '%s' "
                      + "ORDER BY ordinal_position") % (table_name, self._dbname)
        with self.connect() as cu:
            cu.execute(index_query)
            log.debug("save_table_info: index_query: %s", index_query)
            for row in cu.fetchall():
                log.debug("save_table_info: index_query: got row: %s", row)
                indexed_columns[row[0]] = True
                
            cu.execute(main_query)
            col_info = []
            log.debug("save_table_info: main_query: rowcount: %d", cu.rowcount)
            for row in cu.fetchall():
                log.debug("save_table_info: appending raw row: %s", row)
                lrow = list(row)
                lrow.append(indexed_columns.get(row[0], False))
                log.debug("save_table_info: appending row: %s", lrow)
                col_info.append(ColumnInfo(*lrow))
        self.col_info_from_table_name[table_name] = col_info
        log.debug("_save_table_info: #2 table_name: %s, returning %s", table_name,
                  pprint.pformat(col_info))
        return col_info

    def _typeForMySQL(self, typeName):
        return typeName in ('date', 'datetime', 'point')
    
    def _convert(self, col_info_block, row_data):
        """ Convert each item into a string.  Then return an array of items.
        """
        new_row_data = []
        idx = 0
        for value in row_data:
            col_info = col_info_block[idx]
            type = col_info.type.lower()
            log.debug("_convert: value: %s, type:%s", value, type)
            if type == u'int':
                if value is None:
                    new_row_data.append("")
                else:
                    try:
                        new_row_data.append("%d" % value)
                    except TypeError:
                        log.error("Can't append value as int: %r", value)
                        new_row_data.append("%r" % value)
            elif type == u'float':
                new_row_data.append("%g" % value)
            elif (type in (u'string', u'text', u'enum')
                  or 'varchar' in type
                  or type.startswith('char')
                  or type.startswith('character')):
                new_row_data.append(value)
            elif self._typeForMySQL(type):
                new_row_data.append(str(value))
            elif type == 'blob':
                # To get the data of a blob:
                # len(value) => size, str(value) => str repr,
                # but how would we know how to represent it?
                if value is None:
                    log.info("blob data is: None")
                    value = ""
                new_row_data.append("<BLOB: %d chars>" % (len(value),))
            else:
                log.debug("  unrecognized type: %s", type)
                if not _unrec_types.has_key(type):
                    log.info("While converting MySQL values: column %s has an unrecognized type of %s", col_info.column_name, type)
                    _unrec_types[type] = 1
                new_row_data.append('%r' % value)
            idx += 1
        return new_row_data

    def _convertAndJoin(self, names, sep):
        # Return a string of form <<"name1 = ? <sep> name2 = ? ...">>
        return sep.join([("%s = %%s" % name) for name in names])
            
    # GENERIC?
    def getRawRow(self, table_name, key_names, key_values, convert_blob_values=True):
        fixed_table_name = self._qualifyTableName(table_name)
        key_names_str = self._convertAndJoin(key_names, " AND ")
        query = "select * from %s where %s" %  (fixed_table_name, key_names_str)
        with self.connect() as cu:
            cu.execute(query, key_values)
            row = cu.fetchone()
        str_items = []
        if convert_blob_values:
            col_info_block = self._save_table_info(table_name)
        idx = 0
        for item in row:
            if item is None:
                str_items.append("")
            elif convert_blob_values and columnTypeIsBlob(col_info_block[idx].type):
                str_items.append("<BLOB: %d chars>" % (len(item),))
            else:
                str_items.append(str(item))
            idx += 1
        return len(str_items), str_items

    #TODO: Generic?
    def _getRowIdentifier(self, table_name, row_to_delete):
        table_name = self._qualifyTableName(table_name)
        col_info_block = self.get_table_info(table_name)
        key_names = []
        key_values = []
        idx = 0
        for col_info in col_info_block:
            if col_info.is_primary_key:
                key_names.append(col_info.name)
                key_values.append(row_to_delete[idx])
            idx += 1
        if key_names:
            condition = " and ".join(["%s = ?" % (k,) for k in key_names])
        else:
            for col_info in col_info_block:
                if col_info.type != "BLOB":
                    key_names.append(col_info.name)
                    key_values.append(row_to_delete[idx])
                idx += 1
            condition = " and ".join(["%s = ?" % (k,) for k in key_names])
        return condition, key_values

    def deleteRowByKey(self, table_name, key_names, key_values):
        table_name = self._qualifyTableName(table_name)
        condition = " and ".join(["%s = %%s" % kname for kname in key_names])
        with self.connect(commit=True) as cu:
            try:
                cu.execute("delete from %s where %s" % (table_name, condition), key_values)
            except:
                log.exception("mysql deleteRowByKey failed")
                res = False
            else:
                res = True
        return res

    def insertRowByNamesAndValues(self, table_name, target_names, target_values):
        table_name = self._qualifyTableName(table_name)
        cmd = "insert into %s (%s) values (%s)" % (     table_name,
                                                   ", ".join(target_names),
                                                   ", ".join(['%s'] * len(target_names)))
        with self.connect(commit=True) as cu:
            cu.execute(cmd, target_values)
            return True

    def runCustomQuery(self, resultsManager, query):
        try:
            dbxlib.CommonDatabase.runCustomQuery(self, resultsManager, query)
        except TypeError, ex:
            # '%' chars need to be escaped with MySQL (might be fixed one day)
            if 'not enough arguments for format string' in str(ex):
                dbxlib.CommonDatabase.runCustomQuery(self, resultsManager,
                                                     query.replace("%", "%%"))

    def updateRow(self, table_name, target_names, target_values,
                                      key_names, key_values):
        table_name = self._qualifyTableName(table_name)
        target_names_str = self._convertAndJoin(target_names, ",")
        key_names_str = self._convertAndJoin(key_names, " AND ")
        cmd = "update %s set %s where %s" % (table_name, target_names_str,
                                             key_names_str)
        args = tuple(target_values + key_values)
        with self.connect(commit=True) as cu:
            try:
                cu.execute(cmd, args)
                res = True
            except Exception, ex:
                log.exception("dbx_psycopg::updateRow failed")
                res = False
        return res

    # Custom query methods -- these use callbacks into the
    # methods in the loader, to cut down on slinging data
    # around too much

    # runCustomQuery is in the parent class.

    def executeCustomAction(self, action):
        with self.connect(commit=True) as cu:
            try:
                cu.execute(action)
                res = True
            except Exception, ex:
                log.exception("dbx_psycopg::executeCustomAction failed")
                res = False
        return res

    def getIndexInfo(self, indexName, res):
        XXX # Implement!
        
    def getTriggerInfo(self, triggerName, res):
        XXX # Implement!


