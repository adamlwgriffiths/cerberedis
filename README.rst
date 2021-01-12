CerbeRedis
==========

.. image:: images/cerberedis.png

De/serialise `Cerberus <https://github.com/pyeve/cerberus>`_ data to a `Redis <http://redis.io/>`_ database.

Installation
------------

.. code-block:: bash

    $ pip install cerberedis


Limitations
-----------

* Containers cannot be nested. Ie. lists and sets cannot contain lists, sets, or dicts.

Example
-------

A basic example:

.. code-block:: python

    from cerberus import Validator, TypeDefinition
    from redis import Redis
    from cerberedis import CerbeRedis

    schema = {
        'name': {'type': 'string', 'required': True},
        'email': {'type': 'string', 'required': True},
    }
    data = {'name': 'Bob', 'email': 'bob@example.com'}

    # connect to our redis server
    r = Redis()
    db = CerbeRedis(r)

    # save the schema under the type name 'User'
    # with a document id of 1
    model_name, id = 'User', 1
    db.save(model_name, schema, id, document)

    # print the key the model is stored under
    
    print(db.key(model_name, id))

    # reload the document
    loaded_document = db.load(model_name, schema, id)


A full featured example, including custom types:

.. code-block:: python

    from ipaddress import ip_address, IPv4Address, IPv6Address
    from datetime import date, datetime
    from cerberus import Validator, TypeDefinition
    from redis import Redis
    # alternatively use redis_mock (https://github.com/adamlwgriffiths/redis-mock)
    # from redis_mock import Redis
    from cerberedis import CerbeRedis

    # Add custom types to both Cerberus
    # https://docs.python-cerberus.org/en/stable/customize.html
    Validator.types_mapping.update({
        'ipaddress': TypeDefinition('ipaddress', (IPv4Address, IPv6Address), ()),
        'ipv4address': TypeDefinition('ipv4address', (IPv4Address,), ()),
        'ipv6address': TypeDefinition('ipv6address', (IPv6Address,), ()),
    })
    # Provide CerbeRedis with information on how to handle these new Cerberus types
    CerbeRedis.rules.update({
        # dictionary of: <cerberus type name>: [to redis function, from redis function]
        'ipaddress': [lambda x: str(x), lambda x: ip_address(x.decode('utf-8'))],
        'ipv4address': [lambda x: str(x), lambda x: IPv4Address(x.decode('utf-8'))],
        'ipv6address': [lambda x: str(x), lambda x: IPv6Address(x.decode('utf-8'))],
    })

    # An example Cerberus schema that uses all the built-in Cerberus types
    # and the custom ones we defined above
    schema = {
        'boolean': {'type': 'boolean', 'required': True},
        'binary': {'type': 'binary', 'required': True},
        'date': {'type': 'date', 'required': True},
        'datetime': {'type': 'datetime', 'required': True},
        'float': {'type': 'float', 'required': True},
        'integer': {'type': 'integer', 'required': True},
        'number': {'type': 'number', 'required': True},
        'string': {'type': 'string', 'required': True},
        'dict': {'type': 'dict', 'schema': {
            'dict_a': {'type': 'string', 'required': True},
            'dict_b': {'type': 'integer', 'required': True},
        }},
        'list': {'type': 'list', 'schema': {'type': 'integer'}},
        'set': {'type': 'set', 'schema': {'type': 'string'}},
        'ipv4address': {'type': 'ipv4address', 'required': True},
    }

    data = {
        'boolean': True,
        'binary': b'123',
        'date': date.today(),
        'datetime': datetime.now(),
        'float': 1.23,
        'integer': 456,
        'number': 789.0,
        'string': 'abcdefg',
        'dict': {
            'dict_a': 'dict_a_value',
            'dict_b': 9999,
        },
        'list': [1,2,3,4,5],
        'set': {'a', 'b', 'c'},
        'ipv4address': IPv4Address('127.0.0.1'),
    }

    # Use cerberus to validate and normalise the data
    validator = Validator(schema)
    document = validator.normalized(data)
    if not document:
        raise ValueError(str(validator.errors))

    # connect to our redis server
    r = Redis()
    db = CerbeRedis(r)
    model_name, id = 'TestModel', 1
    db.save(model_name, schema, id, document)

    # reload the document
    loaded_document = db.load(model_name, schema, id)

    # verify the data is laid out how we expect
    assert document == loaded_document

Future Work
-----------

* Use a Redis Pipeline for the load function
