#!/usr/bin/env python

import sqlite3

class TodoItem:

    def __init__(self, creation=None, lastupdate=None, updates=None):
        self._creation = creation
        self._lastupdate = lastupdate
        self._updates = updates
        self._title = None
        self._deadline = None
        self._completion = None
        self._tags = []

    @classmethod
    def fromRow(cls, row):
        item = cls(row['creation'], row['lastupdate'], row['updates'])
        item.setTitle(row['title'])
        item.setDeadline(row['deadline'])
        item.setCompletion(row['completion'])
        return item

    def setDeadline(self, deadline):
        self._deadline = deadline
        return self

    def setTitle(self, title):
        self._title = title
        return self

    def setCompletion(self, completion):
        self._completion = completion
        return self

    def setTags(self, tags):
        self._tags = []
        for t in tags:
            self._tags.append(t)
        return self

    @property
    def creation(self):
        return self._creation

    @property
    def lastupdate(self):
        return self._lastupdate

    @property
    def deadline(self):
        return self._deadline

    @property
    def updates(self):
        return self._updates

    @property
    def title(self):
        return self._title

    @property
    def completion(self):
        return self._completion

    @property
    def tags(self):
        return self._tags[::]


class TodoDatabase:

    def __init__(self, dbfile):
        self._conn = sqlite3.connect(dbfile)
        self._conn.executescript("""
CREATE TABLE IF NOT EXISTS todo (
    creation INTEGER DEFAULT CURRENT_TIMESTAMP,
    lastupdate INTEGER DEFAULT CURRENT_TIMESTAMP,
    updates INTEGER DEFAULT 0,
    deadline INTEGER,
    title TEXT NOT NULL ON CONFLICT ABORT,
    completion INTEGER DEFAULT 0
        CHECK (completion >= 0 AND completion <= 100)
);

CREATE INDEX IF NOT EXISTS index_deadline ON todo (deadline);

CREATE INDEX IF NOT EXISTS index_completion ON todo (completion);


CREATE TABLE IF NOT EXISTS tags (
    todokey INTEGER REFERENCES todo (rowid) ON DELETE CASCADE,
    tag TEXT
);

CREATE INDEX IF NOT EXISTS index_tag_todokey ON tags (tag, todokey);

CREATE INDEX IF NOT EXISTS index_todokey ON tags (todokey);


CREATE TRIGGER IF NOT EXISTS trigger_update_todo UPDATE ON todo
BEGIN
    UPDATE todo SET updates = OLD.updates + 1 WHERE rowid = OLD.rowid;
    UPDATE todo SET lastupdate = CURRENT_TIMESTAMP WHERE rowid = OLD.rowid;
END;

CREATE TRIGGER IF NOT EXISTS trigger_update_tags UPDATE ON tags
BEGIN
    UPDATE todo SET updates = updates + 1 WHERE rowid == OLD.todokey;
    UPDATE todo SET lastupdate = CURRENT_TIMESTAMP WHERE rowid = OLD.rowid;
END;

CREATE TRIGGER IF NOT EXISTS trigger_insert_tags INSERT ON tags
BEGIN
    UPDATE todo SET updates = updates + 1 WHERE rowid == NEW.todokey;
    UPDATE todo SET lastupdate = CURRENT_TIMESTAMP WHERE rowid = NEW.rowid;
END;

CREATE TRIGGER IF NOT EXISTS trigger_delete_tags DELETE ON tags
BEGIN
    UPDATE todo SET updates = updates + 1 WHERE rowid == OLD.todokey;
    UPDATE todo SET lastupdate = CURRENT_TIMESTAMP WHERE rowid = OLD.rowid;
END;
        """)

    def add(self, item):
        c = self._conn.cursor()
        #c.execute("BEGIN")
        c.execute("""
INSERT INTO todo (deadline, title, completion) VALUES (?, ?, ?)
        """, (item.deadline, item.title, item.completion))
        print str(item)
        rowid = c.lastrowid
        for tag in item.tags:
                c.execute("""
INSERT INTO tags (todokey, tag) VALUES (?, ?)
                """, (rowid, tag))
        #c.execute("COMMIT")
        return rowid

    def get(self, idlist=[]):
        c = self._conn.cursor()
        query = "SELECT * FROM todo"
        for rowid in idlist:
            query += " WHERE creation IN ({seq})".format(
              seq=','.join(['?'] * len(args)))
        cursor.execute(query, idlist)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            item = TodoItem.fromRow(row)
            c.execute("SELECT tag FROM tags WHERE todokey = ?", row['rowid'])
            tags = [t[0] for t in c.fetchall()]
            item.setTags(tags)
            result.append(item)
        return result


todo = TodoDatabase('todo.sqlite')
item1 = TodoItem().setTitle("upgrade push2mob").setCompletion(20).setTags(['#python', '#sqlite'])
item2 = TodoItem().setTitle("finish up this program").setCompletion(20).setTags(['#python', '#sqlite'])
todo.add(item1)
todo.add(item2)
