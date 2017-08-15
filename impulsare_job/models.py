import datetime

from peewee import BooleanField, CharField, DateTimeField, ForeignKeyField, IntegerField, TextField
from peewee import Model, Proxy
from playhouse.sqlite_ext import SqliteExtDatabase


database_proxy = Proxy()


def open_db(db_name: str):
    db = SqliteExtDatabase(db_name)
    database_proxy.initialize(db)

    return db


class BaseModel(Model):
    def __init__(self, **kwargs):
        if self.table_exists() is False:
            self.create_table()

        Model.__init__(self, **kwargs)

    class Meta:
        database = database_proxy


class Job(BaseModel):
    date_entered = DateTimeField(default=datetime.datetime.now)
    name = CharField(unique=True)
    description = TextField(null=True)
    active = BooleanField(default=True)
    mode = TextField(default='c')
    input = TextField(null=True)
    input_parameters = TextField(null=True)
    output = TextField(null=True)
    output_parameters = TextField(null=True)
    priority = IntegerField(default=1)


class Hook(BaseModel):
    date_entered = DateTimeField(default=datetime.datetime.now)
    name = CharField()
    description = TextField(null=True)
    active = BooleanField(default=True)
    method = TextField()
    when = TextField()
    priority = IntegerField(default=1)
    job = ForeignKeyField(Job)


class Field(BaseModel):
    date_entered = DateTimeField(default=datetime.datetime.now)
    input = TextField(null=True)
    output = TextField(null=True)
    job = ForeignKeyField(Job)


class Rule(BaseModel):
    date_entered = DateTimeField(default=datetime.datetime.now)
    name = CharField()
    description = CharField(null=True)
    active = BooleanField(default=True)
    method = CharField()
    params = TextField(null=True)
    blocking = BooleanField(default=False)
    priority = IntegerField(default=1)
    field = ForeignKeyField(Field)
