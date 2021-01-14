import unittest
from ipaddress import ip_address, IPv4Address, IPv6Address
from datetime import date, datetime
from cerberus import Validator, TypeDefinition
# use either a real redis server, or the mock one
#from redis import Redis
from redis_mock import Redis
from cerberedis import CerbeRedis

def str_to_bytes(s):
    return bytes(s, 'utf-8')

class TestRedisDB(unittest.TestCase):
    @classmethod
    def register_custom_types(cls):
        Validator.types_mapping.update({
            'ipaddress': TypeDefinition('ipaddress', (IPv4Address, IPv6Address), ()),
            'ipv4address': TypeDefinition('ipv4address', (IPv4Address,), ()),
            'ipv6address': TypeDefinition('ipv6address', (IPv6Address,), ()),
        })
        CerbeRedis.rules.update({
            'ipaddress': [lambda x: str(x), lambda x: ip_address(x.decode('utf-8'))],
            'ipv4address': [lambda x: str(x), lambda x: IPv4Address(x.decode('utf-8'))],
            'ipv6address': [lambda x: str(x), lambda x: IPv6Address(x.decode('utf-8'))],
        })

    @classmethod
    def setUpClass(cls):
        cls.register_custom_types()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.redis = Redis(db=10)
        self.redis.flushdb()

    def tearDown(self):
        self.redis.flushdb()

    def test_containers_arent_nestable(self):
        '''list, set containers may not have another container (list, set, dict) inside them
        verify that these are correctly picked up and an error thrown
        '''
        def test_raises(schema, data):
            validator = Validator(schema)
            document = validator.normalized(data)
            self.assertIsNotNone(document)

            r = Redis()
            db = CerbeRedis(r)
            with self.assertRaises(TypeError):
                db.save('test', validator.schema, 1, document)

        def test_list():
            schema = {'field': {'type': 'list', 'schema': {'type': 'list', 'schema': {'type': 'integer'}}}}
            data = {'field': [[1, 2, 3], [4, 5, 6]]}
            test_raises(schema, data)

            schema = {'field': {'type': 'list', 'schema': {'type': 'set', 'schema': {'type': 'integer'}}}}
            data = {'field': [{1, 2, 3}, {4, 5, 6}]}
            test_raises(schema, data)

            schema = {'field': {'type': 'list', 'schema': {'type': 'dict', 'schema': {'sub-field': {'type': 'string', 'required': True}}}}}
            data = {'field': [{'sub-field': 'a'}, {'sub-field': 'b'}]}
            test_raises(schema, data)

        test_list()
        # we don't have to test set
        # as list/set/dict are unhashable
        # and cannot be added to a set

    def test_fields(self):
        db = CerbeRedis(self.redis)

        def save(name, schema, id, data):
            db.save(name, schema, id, data)
        def load(name, schema, id):
            return db.load(name, schema, id)
        def test_save_load(name, schema, id, data):
            save(name, schema, id, data)
            loaded = load(name, schema, id)
            self.assertEqual(data, loaded)

        # test encoding / decoding of each field type
        schema = {'value': {'type': 'boolean', 'required': True}}
        test_save_load('bool', schema, 1, {'value': True})
        test_save_load('bool', schema, 1, {'value': False})

        schema = {'value': {'type': 'binary', 'required': True}}
        test_save_load('bool', schema, 1, {'value': b'123'})
        test_save_load('bool', schema, 1, {'value': b''})

        schema = {'value': {'type': 'date', 'required': True}}
        test_save_load('date', schema, 1, {'value': date.today()})
        test_save_load('date', schema, 1, {'value': date(day=1, month=1, year=1)})

        schema = {'value': {'type': 'datetime', 'required': True}}
        test_save_load('datetime', schema, 1, {'value': datetime.now()})
        test_save_load('datetime', schema, 1, {'value': datetime(day=1, month=1, year=1)})

        schema = {'value': {'type': 'integer', 'required': True}}
        test_save_load('integer', schema, 1, {'value': 1})
        test_save_load('integer', schema, 1, {'value': 0})
        test_save_load('integer', schema, 1, {'value': -1})

        schema = {'value': {'type': 'float', 'required': True}}
        test_save_load('float', schema, 1, {'value': 1.0})
        test_save_load('float', schema, 1, {'value': 0.0})
        test_save_load('float', schema, 1, {'value': -1.0})

        schema = {'value': {'type': 'number', 'required': True}}
        test_save_load('number', schema, 1, {'value': 1})
        test_save_load('number', schema, 1, {'value': 0})
        test_save_load('number', schema, 1, {'value': -1})
        test_save_load('number', schema, 1, {'value': 1.0})
        test_save_load('number', schema, 1, {'value': 0.0})
        test_save_load('number', schema, 1, {'value': -1.0})

        schema = {'value': {'type': 'string', 'required': True}}
        test_save_load('string', schema, 1, {'value': ''})
        test_save_load('string', schema, 1, {'value': 'スパム'})
        test_save_load('string', schema, 1, {'value': '123'})
        with self.assertRaises(AssertionError):
            test_save_load('string', schema, 1, {'value': b'123'})

    def test_end_to_end(self):
        # this schema covers all the basic types, and the custom types we've defined
        schema = {
            # basic types
            'boolean': {'type': 'boolean'},
            'binary': {'type': 'binary'},
            'date': {'type': 'date'},
            'datetime': {'type': 'datetime'},
            'float': {'type': 'float'},
            'integer': {'type': 'integer'},
            'number': {'type': 'number'},
            'string': {'type': 'string'},
            # containers
            'dict': {'type': 'dict', 'schema': {
                'field_a': {'type': 'string'},
                'field_b': {'type': 'integer'},
            }},
            'list': {'type': 'list', 'schema': {'type': 'integer'}},
            'set': {'type': 'set', 'schema': {'type': 'string'}},
            # custom types
            'ipaddress': {'type': 'ipaddress'},
            'ipv4address': {'type': 'ipv4address'},
            'ipv6address': {'type': 'ipv6address'},
        }
        date_today = date.today()
        datetime_now = datetime.now()
        data = {
            'boolean': True,
            'binary': b'123',
            'date': date_today,
            'datetime': datetime_now,
            'float': 1.23,
            'integer': 456,
            'number': 789.0,
            'string': 'abcdefg',
            'dict': {
                'field_a': 'field_a_value',
                'field_b': 9999,
            },
            'list': [1,2,3,4,5],
            'set': {'a', 'b', 'c'},
            'ipaddress': ip_address('127.0.0.1'),
            'ipv4address': IPv4Address('127.0.0.1'),
            'ipv6address': IPv6Address('::1'),
        }
        name, id = 'Test', 1

        # create a cerberus Validator
        # then run our data through the normaliser
        # this way we mimic the full cerberos pipeline
        # even though with our schema nothing should change
        validator = Validator(schema)
        document = validator.normalized(data)
        self.assertIsNotNone(document)

        # save the database
        db = CerbeRedis(self.redis)
        db.save(name, validator.schema, id, document)

        key = db.key(name, id)
        self.assertEqual(key, f'{name}::{id}')

        # reload the document
        loaded_document = db.load(name, validator.schema, id)

        # verify the data is laid out how we expect
        self.assertEqual(document, loaded_document)

        # check the keys are what we expect
        # primary hash
        self.assertEqual(self.redis.hget('Test::1', 'boolean'), b'1')
        self.assertEqual(self.redis.hget('Test::1', 'binary'), b'123')
        self.assertEqual(self.redis.hget('Test::1', 'date'), str_to_bytes(date_today.isoformat()))
        self.assertEqual(self.redis.hget('Test::1', 'datetime'), str_to_bytes(datetime_now.isoformat()))
        self.assertEqual(self.redis.hget('Test::1', 'float'), b'1.23')
        self.assertEqual(self.redis.hget('Test::1', 'integer'), b'456')
        self.assertEqual(self.redis.hget('Test::1', 'number'), b'789.0')
        self.assertEqual(self.redis.hget('Test::1', 'string'), b'abcdefg')
        self.assertEqual(self.redis.hget('Test::1', 'ipaddress'), b'127.0.0.1')
        self.assertEqual(self.redis.hget('Test::1', 'ipv4address'), b'127.0.0.1')
        self.assertEqual(self.redis.hget('Test::1', 'ipv6address'), b'::1')
        # child hash
        self.assertEqual(self.redis.hget('Test::1::dict', 'field_a'), b'field_a_value')
        self.assertEqual(self.redis.hget('Test::1::dict', 'field_b'), b'9999')
        # list
        self.assertEqual(self.redis.lrange('Test::1::list', 0, -1), [b'1',b'2',b'3',b'4',b'5'])
        # set
        self.assertEqual(self.redis.smembers('Test::1::set'), {b'a',b'b',b'c'})


        # serialise with no values
        # there are no required values so this should also work
        document = validator.normalized({})
        self.assertIsNotNone(document)

        db.save(name, validator.schema, id, document)

    def test_not_found(self):
        schema = {
            # basic types
            'boolean': {'type': 'boolean'},
            'binary': {'type': 'binary'},
            'date': {'type': 'date'},
            'datetime': {'type': 'datetime'},
            'float': {'type': 'float'},
            'integer': {'type': 'integer'},
            'number': {'type': 'number'},
            'string': {'type': 'string'},
            # containers
            'dict': {'type': 'dict', 'schema': {
                'field_a': {'type': 'string'},
                'field_b': {'type': 'integer'},
            }},
            'list': {'type': 'list', 'schema': {'type': 'integer'}},
            'set': {'type': 'set', 'schema': {'type': 'string'}},
            # custom types
            'ipaddress': {'type': 'ipaddress'},
            'ipv4address': {'type': 'ipv4address'},
            'ipv6address': {'type': 'ipv6address'},
        }
        db = CerbeRedis(self.redis)
        validator = Validator(schema)
        name, id = 'Test', 1
        document = db.load(name, validator.schema, id)
        self.assertIsNone(document)


if __name__ == '__main__':
    unittest.main()
