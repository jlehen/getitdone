#!/usr/bin/env python2
#
# Copyright (c) 2013, Jeremie Le Hen <jeremie@le-hen.org>
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met: 
# 
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer. 
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution. 
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import getopt
import os
import tempfile
import time
import sqlite3
import subprocess
import sys

# TODO: transaction for tags

KNOWN_DATE_FORMATS = (
    '%y/%m/%d', '%Y/%m/%d', '%d/%m/%Y',
    '%y-%m-%d', '%Y-%m-%d', '%d-%m-%Y',
    '%y%m%d', '%Y%m%d'
)
DB_FILE = os.environ["HOME"] + "/.getitdone.sqlite"

def printTodoItem(todoitem):
    rowid = " - "
    if todoitem.rowid.get() is not None:
        rowid = "%3d" % todoitem.rowid.get()
    completion = ""
    if todoitem.completion.get() is not None:
        completion = "%3d%%" % todoitem.completion.get()
    priority = ""
    if todoitem.priority.get() is not None:
        priority = "%2d!" % todoitem.priority.get()
    deadline = ""
    if todoitem.deadline.get() is not None:
        deadline = time.strftime('@%y/%m/%d',
          time.localtime(todoitem.deadline.get()))
    print "[%3s] %3s %-4s %-9s %s %s" % \
      (rowid, priority, completion, deadline,
        ','.join(todoitem.tags.get()), todoitem.title.get())

class TodoItem(object):

    class ROProperty(object):

        def __init__(self, value=None):
            self._value = value

        def get(self):
            return self._value

    class RWProperty(ROProperty):

        def __init__(self, value=None):
            super(TodoItem.RWProperty, self).__init__(value)
            self.modified = False

        def set(self, value):
            self._value = value
            self.modified = True

        def isModified(self):
            return self.modified


    class RWDProperty(RWProperty):

        def unset(self):
            self._value = None
            self.modified = True


    class RWSet(object):

        def __init__(self, values=[]):
            self._oset = set(values)
            self._set = set(values)
            self._modified = False

        def __iter__(self):
            return iter(self._set)

        def len(self):
            return len(self._set)

        def get(self):
            return self._set.copy()

        def set(self, values):
            self._set = set(values)
            self._modified = True

        def add(self, values):
            if isinstance(values, set) or isinstance(values, frozenset) or \
              isinstance(values, tuple) or isinstance(values, list):
                self._set.update(values)
            else:
                self._set.add(values)
            self._modified = True

        def delete(self, values):
            if isinstance(values, set) or isinstance(values, frozenset) or \
              isinstance(values, tuple) or isinstance(values, list):
                self._set.difference_update(values)
            else:
                self._set.discard(values)
            self._modified = True

        def isModified(self):
            return self._modified

        def difference(self):
            if not self._modified:
                return ((), ())
            plus = self._set - self._oset
            minus = self._oset - self._set
            return (plus, minus)


    def __init__(self, rowid=None, creation=None, lastupdate=None, updates=None):
        self.rowid = TodoItem.ROProperty(rowid)
        self.creation = TodoItem.ROProperty(creation)
        self.lastupdate = TodoItem.ROProperty(lastupdate)
        self.updates = TodoItem.ROProperty(updates)

        self.title = TodoItem.RWProperty()
        self.deadline = TodoItem.RWDProperty()
        self.completion = TodoItem.RWDProperty()
        self.priority = TodoItem.RWDProperty()
        self.description = TodoItem.RWProperty()

        self.tags = TodoItem.RWSet()

    @classmethod
    def fromRow(cls, row):
        item = cls(row['rowid'], row['creation'], row['lastupdate'],
          row['updates'])
        item.title = TodoItem.RWProperty(row['title'])
        item.deadline = TodoItem.RWDProperty(row['deadline'])
        item.completion = TodoItem.RWDProperty(row['completion'])
        item.priority = TodoItem.RWDProperty(row['priority'])
        item.description = TodoItem.RWDProperty(row['description'])
        tags = row['tags'] if row['tags'] is not None else ''
        item.tags = TodoItem.RWSet(tags.split(','))
        return item

    @property
    def modified(self):
        return self.title.isModified() or self.deadline.isModified() or \
          self.completion.isModified() or self.priority.isModified() or \
          self.description.isModified() or self.tags.isModified()

    def update(self, modification):
        if modification.title.isModified():
            self.title.set(modification.title.get())
        if modification.deadline.isModified():
            self.deadline.set(modification.deadline.get())
        if modification.completion.isModified():
            self.completion.set(modification.completion.get())
        if modification.priority.isModified():
            self.priority.set(modification.priority.get())
        if modification.description.isModified():
            self.description.set(modification.description.get())
        if modification.tags.isModified():
            self.tags.set(modifications.tags.get())


class TodoDatabase:

    SQL_TODO_TABLE = """
    CREATE TABLE IF NOT EXISTS todo (
        creation INTEGER DEFAULT CURRENT_TIMESTAMP,
        lastupdate INTEGER DEFAULT CURRENT_TIMESTAMP,
        updates INTEGER DEFAULT 0,
        deadline INTEGER,
        title TEXT NOT NULL ON CONFLICT ABORT,
        description TEXT,
        completion INTEGER DEFAULT 0
            CHECK (completion >= 0 AND completion <= 100),
        priority INTEGER DEFAULT 0
    );
    """

    SQL_TODO_TABLE_INDEXES = """
    CREATE INDEX IF NOT EXISTS index_deadline ON todo (deadline);

    CREATE INDEX IF NOT EXISTS index_completion ON todo (completion);
    """

    SQL_TODO_TABLE_TRIGGERS = """
    CREATE TRIGGER IF NOT EXISTS trigger_update_todo UPDATE ON todo
    BEGIN
        UPDATE todo SET updates = OLD.updates + 1 WHERE rowid = OLD.rowid;

        UPDATE todo
        SET lastupdate = CURRENT_TIMESTAMP
        WHERE rowid = OLD.rowid;
    END;
    """

    SQL_ARCHIVE_TABLE = """
    CREATE TABLE IF NOT EXISTS archive (
        creation INTEGER DEFAULT CURRENT_TIMESTAMP,
        lastupdate INTEGER DEFAULT CURRENT_TIMESTAMP,
        updates INTEGER DEFAULT 0,
        deadline INTEGER,
        title TEXT NOT NULL ON CONFLICT ABORT,
        description TEXT,
        completion INTEGER DEFAULT 0
            CHECK (completion >= 0 AND completion <= 100),
        priority INTEGER DEFAULT 0,
        archivetime INTEGER DEFAULT CURRENT_TIMESTAMP,
        tags TEXT
    );
    """

    SQL_TAGS_TABLE = """
    CREATE TABLE IF NOT EXISTS tags (
        todokey INTEGER REFERENCES todo (rowid) ON DELETE CASCADE,
        tag TEXT
    );
    """

    SQL_TAGS_TABLE_INDEXES = """
    CREATE INDEX IF NOT EXISTS index_tag_todokey ON tags (tag, todokey);

    CREATE INDEX IF NOT EXISTS index_todokey ON tags (todokey);
    """

    SQL_TAGS_TABLE_TRIGGERS = """
    CREATE TRIGGER IF NOT EXISTS trigger_update_tags UPDATE ON tags
    BEGIN
        UPDATE todo SET updates = updates + 1 WHERE rowid == OLD.todokey;

        UPDATE todo
        SET lastupdate = CURRENT_TIMESTAMP
        WHERE rowid = OLD.rowid;
    END;

    CREATE TRIGGER IF NOT EXISTS trigger_insert_tags INSERT ON tags
    BEGIN
        UPDATE todo SET updates = updates + 1 WHERE rowid == NEW.todokey;

        UPDATE todo
        SET lastupdate = CURRENT_TIMESTAMP
        WHERE rowid = NEW.rowid;
    END;

    CREATE TRIGGER IF NOT EXISTS trigger_delete_tags DELETE ON tags
    BEGIN
        UPDATE todo SET updates = updates + 1 WHERE rowid == OLD.todokey;

        UPDATE todo
        SET lastupdate = CURRENT_TIMESTAMP
        WHERE rowid = OLD.rowid;
    END;
    """

    SQL_TEMPLATES_TABLE = """
    CREATE TABLE IF NOT EXISTS templates (
        name TEXT NOT NULL PRIMARY KEY,
        query TEXT NOT NULL
    );
    """

    SQL_JOIN_QUERY = """
    SELECT *
    FROM (
        SELECT
            todo.rowid,
            todo.creation,
            todo.lastupdate,
            todo.updates,
            todo.deadline,
            todo.title,
            todo.description,
            todo.completion,
            todo.priority,
            group_concat(tags.tag, ",") AS tags
        FROM
            todo LEFT OUTER JOIN tags ON todo.rowid = tags.todokey
        GROUP BY todo.rowid
        )
    """

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self, dbfile):
        self._conn = sqlite3.connect(dbfile)
        self._conn.row_factory = TodoDatabase.dict_factory
        self._conn.isolation_level = None
        self._conn.executescript("%s %s %s %s %s %s %s %s" % (  \
            TodoDatabase.SQL_TODO_TABLE,                        \
            TodoDatabase.SQL_TODO_TABLE_INDEXES,                \
            TodoDatabase.SQL_TODO_TABLE_TRIGGERS,               \
            TodoDatabase.SQL_ARCHIVE_TABLE,                     \
            TodoDatabase.SQL_TAGS_TABLE,                        \
            TodoDatabase.SQL_TAGS_TABLE_INDEXES,                \
            TodoDatabase.SQL_TAGS_TABLE_TRIGGERS,               \
            TodoDatabase.SQL_TEMPLATES_TABLE                    \
        ))

    def add(self, item):
        c = self._conn.cursor()
        c.execute("""
        INSERT INTO todo (deadline, title, completion, priority)
        VALUES (?, ?, ?, ?);
        """, (item.deadline.get(), item.title.get(),
           item.completion.get(), item.priority.get()))
        rowid = c.lastrowid
        for tag in item.tags:
                c.execute("""
                INSERT INTO tags (todokey, tag) VALUES (?, ?);
                """, (rowid, tag))
        return rowid

    def update(self, item):
        c = self._conn.cursor()

        columns = []
        if item.title.isModified():
            columns.append(("title", item.title.get()))
        if item.description.isModified():
            columns.append(("description", item.description.get()))
        if item.completion.isModified():
            columns.append(("completion", item.completion.get()))
        if item.deadline.isModified():
            columns.append(("deadline", item.deadline))
        if item.priority.isModified():
            columns.append(("priority", item.priority.get()))
        if len(columns) > 0:
            query = "UPDATE todo SET "
            query += ", ".join(map(lambda t: "%s = ?" % t[0], columns))
            query += " WHERE rowid = ?"
            params = map(lambda t: t[1], columns)
            params.append(item.rowid.get())
            c.execute(query, params)

        tags, untags = item.tags.difference()
        for tag in tags:
            c.execute("""
            INSERT INTO tags (todokey, tag) VALUES (?, ?);
            """, (item.rowid.get(), tag))
        if len(untags) > 0:
            subquery = " OR ".join("tags = ?" * len(untags))
            params = [rowid] + map(lambda x: x, untags)
            c.execute("""
            DELETE FROM tags WHERE rowid = ? AND (%s)
            """ % subquery, params)

    def delete(self, ids):
        c = self._conn.cursor()
        for rowid in ids:
            item = self.get_raw("WHERE rowid = ?", (int(rowid),))[0]
            query = """
            INSERT INTO archive(creation, lastupdate, updates, deadline, title,
                                completion, priority, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            c.execute(query, (item.creation.get(), item.lastupdate.get(),
                item.updates.get(), item.deadline.get(), item.title.get(),
                item.completion.get(), item.priority.get(),
                ','.join(item.tags.get())))
        query = "DELETE FROM todo WHERE rowid IN ({idslist})".format(
            idslist=', '.join(['?'] * len(ids)))
        c.execute(query, ids)
 
    def get_raw(self, querycond, params=[]):
        query = TodoDatabase.SQL_JOIN_QUERY
        query += querycond
        query += ";"
        c = self._conn.cursor()
        c.execute(query, params)
        itemlist = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            itemlist.append(TodoItem.fromRow(row))
        return itemlist


    def get(self, item):
        columns = []
        if item.title.isModified():
            columns.append(("title", "*" + item.title.get() + "*"))
        if item.description.isModified():
            columns.append(("description", "*" + item.description.get() + "*"))
        if item.completion.isModified():
            columns.append(("completion", item.completion.get()))
        if item.deadline.isModified():
            columns.append(("deadline", item.deadline.get()))
        if item.completion.isModified():
            columns.append(("priority", item.priority.get()))


        querycond = map(lambda t: "%s GLOB ?" % t[0], columns)
        params = map(lambda t: t[1], columns)

        if item.tags.isModified():
            querycond.append("""
                rowid IN (
                    SELECT DISTINCT todokey
                    FROM tags
                    WHERE tag IN ({taglist})
                )
            """.format(taglist=', '.join(['?'] * item.tags.len())))
            params += map(lambda x: x, item.tags)

        where = ""
        if len(querycond) > 0:
            where = "WHERE " + " AND ".join(querycond)

        return self.get_raw(where, params)


    def templateadd(self, name, query):
        self._conn.cursor().execute("""
        INSERT INTO templates (name, query) VALUES (?, ?);
        """, (name, query))


    def templatedel(self, name):
        self._conn.cursor().execute("""
        DELETE FROM templates WHERE name = ?;
        """, (name,))


    def templateshow(self, names=[]):
        c = self._conn.cursor()
        query = "SELECT * FROM templates";
        if len(names) > 0:
            query += " WHERE name IN (" + ", ".join(["?"] * len(names)) + ")"
        query += ';'
        c.execute(query, names)
        while True:
            row = c.fetchone()
            if row is None:
                break
            print "[%12s] %s" % (row['name'], row['query']);


    def templaterun(self, name, params):
        c = self._conn.cursor()
        c.execute("""
        SELECT query FROM templates WHERE name = ?;
        """, (name,))
        row = c.fetchone()
        if row is None:
            raise ValueError("Unknown template: %s" % name)
        query = row['query']
        return todo.get_raw(query, params)


def usage(progname):
    print """
Usage: %s [-d dbfile] <command> [args]
Commands:
  schema
  template add <name> <query ...>
  template del <name>
  template show <name ...>
  template run <name> <params ...>
  sql <query ...>
  add/insert <property ...>
  update/set <id> <property ...>
  get/print/list [property ...]
  del/delete/rem/remove <id>
  edit <id>
Property:
  %%n    - completion set to n%%; remove with -%% in update
  !n    - priority set to n; remove with -! in update
  @date - deadline set to date; remove with -@ in update
  #tag  - add tag; remove with -#tag in update
  word  - belongs to title
""" % progname

if __name__ == "__main__":
    progname = sys.argv[0]
    argv = sys.argv[1:]
    dbfile = DB_FILE
    optlist, argv = getopt.getopt(argv, 'd:h')
    for opt, optarg in optlist:
        if opt == '-d':
            dbfile = optarg
        elif opt == '-h':
            usage(progname)
            sys.exit(0)

    os.umask(0077)
    todo = TodoDatabase(dbfile)

    if len(argv) == 0:
        argv.append("help")
    cmd = argv[0]
    argv = argv[1:]
    if len(argv) > 0:
        argv = filter(lambda a: len(a) > 0,
          reduce(lambda x, y: x + y, [arg.split() for arg in argv]))

    if cmd == "help":
        usage(progname)

    if cmd == "schema":
        print "You can run a where query against this table:"
        print TodoDatabase.SQL_JOIN_QUERY
        sys.exit(0)

    if cmd == "template" or cmd == "tmpl":
        cmd = argv[0]
        argv = argv[1:]

        if cmd == "add":
            name = argv[0]
            req = ' '.join(argv[1:])
            todo.templateadd(name, req)

        if cmd == "del" or cmd == "delete":
            name = argv[0]
            todo.templatedel(name)

        if cmd == "show":
            todo.templateshow(argv)

        if cmd == "run":
            name = argv[0]
            for ritem in todo.templaterun(name, argv[1:]):
                printTodoItem(ritem)

        sys.exit(0)

    if cmd == "sql":
        query = " ".join(argv)
        for ritem in todo.get_raw(query):
            printTodoItem(ritem)
        sys.exit(0)

    item = TodoItem()
    title = []
    tags = set()
    untags = set()
    for arg in argv:
        if arg[0] == '#':                               # Add tag
            tags.add(arg)
        elif arg[0:2] == "-#":                          # Remove tag
            untags.add(arg[1:])
        elif arg[0] == '%':                             # Set completion
            item.completion.set(int(arg[1:]))
        elif arg[0:2] == '-%':                          # Unset completion
            item.completion.unset()
        elif arg[0] == '!':                             # Set priority
            item.priority.set(int(arg[1:]))
        elif arg[0:2] == '-!':                          # Unset priority
            item.priority.unset()
        elif arg[0] == '@':                             # Set deadline
            ok = False
            for fmt in KNOWN_DATE_FORMATS:
                try:
                    date = time.strptime(arg[1:], fmt)
                    item.deadline.set(time.mktime(date))
                    ok = True
                    break
                except ValueError:
                    pass
                if not ok:
                    raise ValueError("Unknown date format: %s" % arg[1:])
        elif arg[0:2] == '-@':                          # Set deadline
            item.deadline.unset()
        else:
            title.append(arg)

    if cmd == 'add' or cmd == 'insert':
        item.tags.set(tags)
        if len(title) == 0:
            raise ValueError("Empty title")
        item.title.set(' '.join(title))
        print todo.add(item)
        sys.exit(0)

    if cmd == 'update' or cmd == 'set':
        tagsintersect = tags & untags
        if len(tagsintersect) != 0:
            raise ValueError("Tags & untags intersect: %s" % \
              ', '.join(tagsintersect))

        rowid = title[0]
        title = title[1:]

        itemlist = todo.get_raw("WHERE rowid = ?", (int(rowid),))
        if len(itemlist) == 0:
            raise ValueError("No such item: %s" % rowid)
        curitem = itemlist[0]

        if len(title) > 0:
            item.title.set(' '.join(title))
        curitem.update(item)
        if len(tags) > 0:
            curitem.tags.add(tags)
        if len(untags) > 0:
            curitem.tags.delete(untags)
 
        todo.update(curitem)
        sys.exit(0)
        
    if cmd == 'get' or cmd == 'print' or cmd == 'list':
        if len(title) > 0:
            item.title.set(' '.join(title))
        if len(tags) > 0:
            item.tags.set(tags)

        for ritem in todo.get(item):
            printTodoItem(ritem)
        sys.exit(0)

    if cmd == 'del' or cmd == 'delete':
        ids = []
        for rowid in title:
            ids.append(int(rowid))
        todo.delete(ids)
        sys.exit(0)

    if cmd == 'edit':
        rowid = title[0]

        itemlist = todo.get_raw("WHERE rowid = ?", (int(rowid),))
        if len(itemlist) == 0:
            raise ValueError("No such item: %s" % rowid)
        curitem = itemlist[0]
        f, fname = tempfile.mkstemp()
        f = os.fdopen(f, "w")
        f.write(curitem.description.get() or "")
        f.close()
        editor = os.environ.get("EDITOR") or "/bin/vi"
        mtime0 = os.stat(fname).st_mtime
        subprocess.call("%s %s" % (editor, fname), shell=True)

        mtime1 = os.stat(fname).st_mtime
        if mtime0 == mtime1:
            print "No changes."
            os.unlink(fname)
            sys.exit(0)
        f = open(fname, 'r')
        description = f.read()
        f.close()
        os.unlink(fname)
        curitem.description.set(description)
        todo.update(curitem)
        sys.exit(0)

    print "ERROR: Unknown command: %s" % cmd
    usage(progname)
