import json

from .db import Db
from .models import Field, Job, Hook, Rule


class Reader(Db):
    """Simple reader to get data from an SQLite DB"""

    def __init__(self, config_file: str, job: str):
        Db.__init__(self, config_file)
        try:
            self._job = Job.get(Job.name == job)
        except Exception:
            raise ValueError("Can't retrieve Job {}".format(job))

        self._job.input_parameters = self._convert_json(self._job.input_parameters)
        self._job.output_parameters = self._convert_json(self._job.output_parameters)


    def get_job(self) -> Job:
        """Get the retrieved Job"""

        return self._job


    def get_hooks(self):
        """Get hooks in DB For the current job"""
        # Make sure I have the table created
        # TODO create the tables in another way
        Hook()

        return Hook.select().where(Hook.job == self._job.id).order_by(Hook.priority)


    def get_fields(self):
        """Get rules in DB For the current job"""
        # Make sure I have the table created
        # TODO create the tables in another way
        Field()

        fields = Field.select().where(Field.job == self._job.id).order_by(Field.output)
        for field in fields:
            field.rules = self._get_rules(field.id)

        return fields


    def get_prop(self, prop: str):
        return getattr(self._job, prop)


    def _get_rules(self, field_id: int):
        """Get rules in DB For the current job"""
        # Make sure I have the table created
        # TODO create the tables in another way
        Rule()

        rules = Rule.select().where(Rule.field == field_id).order_by(Rule.priority)
        for rule in rules:
            rule.params = self._convert_json(rule.params)

        return rules


    def _convert_json(self, jsonstr: str):
        if jsonstr == '':
            return {}

        return json.loads(jsonstr)
