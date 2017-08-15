impulsare/job
=============


.. image:: https://travis-ci.org/impulsare/job.svg?branch=master
    :target: https://travis-ci.org/impulsare/job

.. image:: https://scrutinizer-ci.com/g/impulsare/job/badges/quality-score.png?b=master
    :target: https://scrutinizer-ci.com/g/impulsare/job/

.. image:: https://scrutinizer-ci.com/g/impulsare/job/badges/coverage.png?b=master
    :target: https://travis-ci.org/impulsare/job

Overview
--------

A jobs manager, specific to **impulsare**. It reads, write and delete jobs from an sqlite db.

See ``tests/static/`` for examples of configuration.


Installation / Usage
====================
To install use pip:

.. code-block:: bash

    $ pip install --upgrade impulsare-job


Configuration
=============
You need to create a configuration file that contains:

.. code-block:: yaml

    job:
        db: /tmp/test.db # required


Architecture
============
Writer
------
impulsare/job implements a writer to :

- Create / Update jobs (``save()``)
- Delete jobs (``delete()``)
- Add / Remove Hooks (`add_hook()` and ``del_hook()``)
- Add / Remove Fields (``add_field()`` and ``del_field()``)
- Add / Remove Rules related to Fields (``add_rule()`` and ``del_rule()``)


Reader
------
And a Reader to :

- Get a Job
- Get related hooks
- Get related fields + their rules


Properties of a job
-------------------------

.. code-block:: python

    {
        'name': str, # required
        'active': bool, # default : True
        'description': str,
        'priority': int, # default : 1
        'input': str, # required
        'input_parameters': dict,
        'output': str, # required
        'output_parameters': dict,
        'mode': str # c (create), u (update), cu (create/update), d (delete). Default: c
    }


Examples
--------
Create a simple Job (no hooks / rules)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from impulsare_job import Writer


    writer = Writer('/etc/impulsare/config.yml')
    writer.set_prop('name', 'My Job')
    writer.set_prop('input', 'csv')
    writer.set_prop('input_parameters', {'delimiter': 'csv'})
    writer.set_prop('output', 'sql')
    writer.set_prop('output_parameters', {'db': 'test'})
    job = writer.save()


Update a Job
~~~~~~~~~~~~

.. code-block:: python

    from impulsare_job import Writer


    # Lets assume the job id = 1
    writer = Writer('/etc/impulsare/config.yml', 'My Job')
    job = writer.get_job()
    print(job.name)
    # Output: 'My Job'

    # Set the job to Inactive
    writer.set_prop('active', False)
    writer.save()



Verify if a hook exists, else add it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # .... continuation of code above
    if not writer.hook_exists('test'):
        writer.add_hook(name='upload_file', method='upload_file', when='after_process')


Allowed properties for hooks:

.. code-block:: python

    {
        'name': str, # required
        'method': str, # required
        'when': str, # required
        'description': str,
        'active': bool, # Default : True
        'priority': int # Default: 1
    }


Other methods:

- `get_hooks`
- `del_hook`


There is no method `update`, to update a hook, delete it then recreate it.


Verify if a field exists, else update it and add a transformation rule
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Warning** : a field is identified by its ``output`` value that must be unique
(we can't send two values for the same field while we can use the same input field
for various output).


**Field**

.. code-block:: python

    # .... continuation of code above
    if writer.field_exists('firstname'):
        writer.del_field('firstname')

    writer.add_field(input='first_name', output='firstname')


Allowed properties for fields:

.. code-block:: python

    {
        'input': str, # required
        'output': str, # required
    }


Other methods:

- `get_field`
- `get_fields`


There is no method `update`, to update a field, delete it then recreate it.


Add a rule
~~~~~~~~~~

.. code-block:: python

    writer.add_rule(output_field='firstname', name='uppercase', method='uppercase')


Allowed properties for rules:

.. code-block:: python

    {
        'name': str, # required
        'method': str, # required
        'description': str,
        'active': bool, # Default : True
        'params': list,
        'blocking': bool, # Default : False
        'priority': int # Default: 1
    }


Other methods:

- `del_rule`
- `get_rules`
- `rule_exists`


There is no method ``update``, to update a rule, delete it then recreate it.


Retrieve a Job, its hooks and fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from impulsare_job import Reader


    Reader = Reader('/etc/impulsare/config.yml', 'My Job')
    job = Reader.get_job()
    hooks = Reader.get_hooks()
    fields = Reader.get_fields() # Get rules for first field : rules = fields[0].rules


Development & Tests
===================

.. code-block:: bash

    $ pip install -r requirements.txt
    $ pip install -r requirements-dev.txt
    $ py.test



TODO
----
Don't check if table exists on each model but do it on app installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To get the DB :

.. code-block:: python

    from impulsare_job import models


    db = models.get_db('/etc/impulsare/config.yml')
    db.create_tables([models.Job, models.Hook, models.Rule])


Refactor writer
~~~~~~~~~~~~~~~
To have a class for hooks, and another for rules.
