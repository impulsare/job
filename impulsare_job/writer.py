import json

from .db import Db
from .reader import Reader
from .models import Field, Job, Hook, Rule
from peewee import DeleteQuery, OperationalError


class Writer(Db):
    """Jobs Writer. Write Jobs to an SQLite file"""

    _required_props = ('name', 'input', 'output')


    def __init__(self, config_file: str, job: str = None):
        Db.__init__(self, config_file)

        self._data = {
            'name': None,
            'active': True,
            'description': None,
            'priority': 1,
            'input': None,
            'input_parameters': {},
            'output': None,
            'output_parameters': {},
            'mode': 'c'
            }

        self._fields = {}
        self._hooks = {}

        if job is not None:
            self._reader = Reader(config_file, job)
            self._job = self._reader.get_job()
            self._populate_data_from_job()
        else:
            self._job = Job()


    def get_job(self) -> Job:
        """Returns the current job state"""

        return self._job


    def get_prop(self, prop: str):
        """Get a property from the data, not saved yet to the job"""

        if prop not in self._job_props_type:
            raise KeyError('{} is not a valid property'.format(prop))

        return self._data[prop]


    def set_prop(self, prop: str, value) -> None:
        """
        Set a value for a property defined in self._data
        We can define specific checks by creating a method with the
        name of the prop (Example: to check 'mode' create '_check_mode')
        """

        check_method = '_check_{}'.format(prop)
        if hasattr(self, check_method) and callable(getattr(self, check_method)):
            getattr(self, check_method)(value)

        self._data[prop] = self._parse_value(prop, value)


    def add_field(self, input: str, output: str):
        if self.field_exists(output):
            raise KeyError('Field {} already exists. Delete it first'.format(output))

        self._fields[output] = {'input': input, 'output': output, 'rules': {}}


    def del_field(self, output_field: str) -> None:
        if self.field_exists(output_field) is False:
            raise KeyError('Field {} does not exist'.format(output_field))

        del self._fields[output_field]


    def field_exists(self, output_field: str) -> bool:
        if output_field in self._fields:
            return True

        return False


    def get_field(self, output_field: str) -> dict:
        if not self.field_exists(output_field):
            raise KeyError('Field {} does not exist'.format(output_field))

        return self._fields[output_field]


    def get_fields(self) -> dict:
        return self._fields


    def add_rule(self, output_field: str, name: str, method: str, description: str = None,
                 active: bool = True, params: list = {}, blocking: bool = False,
                 priority: int = 1) -> None:

        if self.rule_exists(output_field, name):
            raise KeyError('Rule {} already exists for {}. Delete it first'.format(name, output_field))

        # Add all real parameters
        self._fields[output_field]['rules'][name] = {
            'name': name, 'method': method, 'description': description,
            'active': active, 'params': params, 'blocking': blocking,
            'priority': priority
            }


    def del_rule(self, output_field: str, rule: str) -> None:
        if not self.rule_exists(output_field, rule):
            raise KeyError('Rule {} does not exist for {}'.format(rule, output_field))

        del self._fields[output_field]['rules'][rule]


    def get_rules(self, output_field: str) -> dict:
        if self.field_exists(output_field) is False:
            raise KeyError('Field {} does not exists. Cannot get rules'.format(output_field))


        return self._fields[output_field]['rules']


    def rule_exists(self, output_field: str, rule: str) -> bool:
        if self.field_exists(output_field) is False:
            raise KeyError('Field {} does not exists. Add it first'.format(output_field))

        if rule in self._fields[output_field]['rules']:
            return True

        return False


    def hook_exists(self, name: str) -> bool:
        if name in self._hooks:
            return True

        return False


    def add_hook(self, name: str, method: str, when: str, description: str = None,
                 active: bool = True, priority: int = 1) -> None:

        if self.hook_exists(name):
            raise KeyError('Hook {} already exists. Delete it first'.format(name))

        # Add all real parameters
        self._hooks[name] = {'name': name, 'method': method, 'when': when,
                             'description': description, 'active': active, 'priority': priority}


    def get_hook(self, name: str) -> list:
        if not self.hook_exists(name):
            raise KeyError('Hook {} does not exist'.format(name))

        return self._hooks[name]


    def get_hooks(self):
        return self._hooks


    def del_hook(self, name: str) -> None:
        if not self.hook_exists(name):
            raise KeyError('Hook {} does not exist'.format(name))

        del self._hooks[name]


    def save(self) -> Job:
        self._verify_required_values()

        for prop in self._job_props_type:
            setattr(self._job, prop, self._data[prop])

        try:
            self._job.save()
            self._add_hooks_to_db()
            self._add_fields_to_db()

            return self._job
        except Exception as e:
            raise RuntimeError("Can't insert the job '{}' ({})".format(self._job.name, e))


    def delete(self):
        self._job.delete_instance()


    def _add_hooks_to_db(self):
        # First delete all hooks to avoid spending too much time
        # updating, deleting, etc ...
        try:
            DeleteQuery(Hook).where(Hook.job == self._job.id).execute()
        except OperationalError as e:
            pass

        hooks_added = list()
        for hook, params in self._hooks.items():
            hook = Hook(name=hook, when=params['when'], method=params['method'],
                        priority=params['priority'], description=params['description'],
                        active=params['active'], job=self._job)
            hook.save()
            hooks_added.append(hook.id)

        return hooks_added


    def _add_fields_to_db(self):
        # First delete all rules linked ot fields + fields
        # to avoid spending too much time updating, deleting, etc ...
        try:
            fields = Field.select().where(Field.job == self._job)
            for field in fields:
                field.delete_instance(recursive=True)
        except OperationalError as e:
            pass

        fields_added = list()
        for field, params in self._fields.items():
            field = Field(input=params['input'], output=params['output'], job=self._job)
            field.save()
            fields_added.append(field.id)

            if params['rules'] is not {}:
                self._add_rules_to_db(field.id, params['rules'])

        return fields_added


    def _add_rules_to_db(self, field_id: int, rules: dict):
        for rule, params in rules.items():
            rule = Rule(
                name=rule, method=params['method'], description=params['description'],
                active=params['active'], params=json.dumps(params['params']),
                blocking=params['blocking'], priority=params['priority'], field=field_id
                )

            rule.save()


    def _check_mode(self, mode: str) -> None:
        if mode not in ['c', 'u', 'cu', 'd']:
            raise ValueError('{} is not a valid mode (c - u - cu - d)'.format(mode))


    def _parse_value(self, prop: str, value):
        if prop not in self._job_props_type:
            raise KeyError("Can't set {} as it does not exist in our dict".format(prop))

        expected_type = self.get_job_prop_type(prop)
        if type(value) is not expected_type:
            raise ValueError('{} must be of type {}'.format(prop, expected_type))

        if expected_type is dict:
            value = json.dumps(value)

        return value


    def _populate_data_from_job(self) -> None:
        for prop in self._job_props_type:
            self._data[prop] = self._reader.get_prop(prop)

        self._set_fields_from_job()
        self._set_hooks_from_job()


    def _set_hooks_from_job(self) -> None:
        hooks = self._reader.get_hooks()
        for hook in hooks:
            self.add_hook(hook.name, hook.method, hook.when, hook.description,
                          hook.active, hook.priority)


    def _set_fields_from_job(self) -> None:
        fields = self._reader.get_fields()
        for field in fields:
            self.add_field(input=field.input, output=field.output)
            self._set_rules_from_field(field.output, field.rules)


    def _set_rules_from_field(self, field: str, rules: dict) -> None:
        for rule in rules:
            self.add_rule(field, rule.name, rule.method, rule.description,
                          rule.active, rule.params, rule.blocking, rule.priority)


    def _verify_required_values(self) -> None:
        for prop in self._required_props:
            value = self._data[prop]
            if self.get_job_prop_type(prop) is dict:
                value = None if len(value) == 0 else value

            if value is None:
                raise ValueError('Property {} is required'.format(prop))
