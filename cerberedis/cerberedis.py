from ipaddress import ip_address, IPv4Address, IPv6Address
from datetime import date, datetime
from cerberus import Validator, TypeDefinition


class CerbeRedis(object):
    identity = lambda x: x
    # https://docs.python-cerberus.org/en/stable/validation-rules.html#type
    rules = {
        # to redis, from redis
        'boolean': [lambda x: int(x), lambda x: bool(int(x.decode('utf-8')))],
        'binary': [identity, identity],
        'date': [lambda x: x.isoformat(), lambda x: date.fromisoformat(x.decode('utf-8'))],
        'datetime': [lambda x: x.isoformat(), lambda x: datetime.fromisoformat(x.decode('utf-8'))],
        'float': [identity, lambda x: float(x)],
        'integer': [identity, lambda x: int(x)],
        'number': [identity, lambda x: float(x)],
        'string': [identity, lambda x: x.decode('utf-8')],
    }

    def __init__(self, db, rules=None):
        self.db = db
        rules = rules or {}
        self.rules.update({**rules})

    def key(self, type_name, id):
        return f'{type_name}::{id}'

    def _rules(self, schema):
        schema_type = schema.get('type')
        rules = self.rules.get(schema_type)
        if not rules:
            raise TypeError(f'No rules specified for how to handle type "{schema_type}"')
        return rules

    def lower_field(self, schema, value):
        to_redis, _ = self._rules(schema)
        return to_redis(value)

    def raise_field(self, schema, value):
        _, from_redis = self._rules(schema)
        return from_redis(value)

    def _replace_hash(self, db, key, data):
        db.delete(key)
        for name, value in data.items():
            db.hset(key, name, value)

    def _replace_list(self, db, key, data):
        db.delete(key)
        for value in data:
            db.rpush(key, value)

    def _replace_set(self, db, key, data):
        db.delete(key)
        for value in data:
            db.sadd(key, value)

    def _save(self, db, type_name, schema, id, data):
        '''Save function which supports recursion through it's signature
        '''
        key = self.key(type_name, id)

        # if field is dict, list, set
        # recurse and call with name::id::field
        # dict is a full recursion
        def container_schema(field_name, field_schema):
            item_schema = field_schema.get('schema')
            if not item_schema:
                raise TypeError(f'No schema provided for contents of "{field_name}" with type {field_schema}')
            if item_schema['type'] in ['list', 'set', 'dict']:
                raise TypeError(f'Container schemas are not nestable. "{field_name}" of type "{field_schema["type"]}" has a sub-type of "{item_schema["type"]}"')
            return item_schema
        def save_dict(field_name, field_schema, field_data):
            dict_schema = field_schema.get('schema')
            self._save(db, key, dict_schema, field_name, field_data)
        def save_list(field_name, field_schema, field_data):
            sub_key = f'{key}::{field_name}'
            item_schema = container_schema(field_name, field_schema)
            sub_data = [self.lower_field(item_schema, item) for item in field_data]
            self._replace_list(db, sub_key, sub_data)
        def save_set(field_name, field_schema, field_data):
            sub_key = f'{key}::{field_name}'
            item_schema = container_schema(field_name, field_schema)
            sub_data = {self.lower_field(item_schema, item) for item in field_data}
            self._replace_set(db, sub_key, sub_data)

        containers = {'dict': save_dict, 'list': save_list, 'set': save_set}

        final_data = {}
        for field_name, field_schema in schema.items():
            field_type = field_schema['type']
            field_data = data.get(field_name, {})

            # ignore empty fields
            if field_data is None:
                continue

            # handle containers
            if field_type in containers:
                containers[field_type](field_name, field_schema, field_data)
                continue

            # perform lowering on the field
            # put into our new dict
            lowered_value = self.lower_field(field_schema, field_data)
            final_data[field_name] = lowered_value

        # save the final hash
        self._replace_hash(db, key, final_data)

    def save(self, type_name, schema, id, data):
        '''Saves the model using a transaction
        Throws TypeError on an unsupported schema.
        '''
        p = self.db.pipeline(transaction=True)
        try:
            self._save(p, type_name, schema, id, data)
            p.execute()
        except:
            p.reset()
            raise

    def _load_hash(self, db, key, field):
        return db.hget(key, field)

    def _load_list(self, db, key):
        return db.lrange(key, 0, -1)

    def _load_set(self, db, key):
        return db.smembers(key)

    def _load(self, db, type_name, schema, id):
        key = self.key(type_name, id)

        def container_schema(field_name, field_schema):
            item_schema = field_schema.get('schema')
            if not item_schema:
                raise TypeError(f'No schema provided for contents of "{field_name}" with type {field_schema}')
            if field_schema['type'] in ['list', 'set']:
                if item_schema['type'] in ['list', 'set', 'dict']:
                    raise TypeError(f'Container schemas are not nestable. "{field_name}" of type "{field_schema["type"]}" has a sub-type of "{item_schema["type"]}"')
            return item_schema
        def load_dict(field_name, field_schema):
            item_schema = container_schema(field_name, field_schema)
            return self._load(db, key, item_schema, field_name)
        def load_list(field_name, field_schema):
            sub_key = f'{key}::{field_name}'
            item_schema = container_schema(field_name, field_schema)
            field_data = self._load_list(db, sub_key)
            return [self.raise_field(item_schema, item) for item in field_data]
        def load_set(field_name, field_schema):
            sub_key = f'{key}::{field_name}'
            item_schema = container_schema(field_name, field_schema)
            field_data = self._load_set(db, sub_key)
            return {self.raise_field(item_schema, item) for item in field_data}

        containers = {'dict': load_dict, 'list': load_list, 'set': load_set}

        data = {}
        # load the current hash
        for field_name, field_schema in schema.items():
            field_type = field_schema['type']

            # containers are handled differently
            if field_type in containers:
                value = containers[field_type](field_name, field_schema)
                data[field_name] = value
                continue

            field_data = self._load_hash(db, key, field_name)

            # ignore empty fields
            if field_data is None:
                continue

            # raise the type
            field_data = self.raise_field(field_schema, field_data)
            data[field_name] = field_data

        return data

    def load(self, type_name, schema, id):
        # TODO: we could pipeline this, but this is easier for now
        return self._load(self.db, type_name, schema, id)
