from unittest import TestCase

from peewee import SqliteDatabase

from sequin import SequinEvent, SequinEntity, register_database

import json

import os

from sequin.errors import EntityStaleError


class TestSetup(TestCase):
    def test_entity_name_should_work_with_override(self):
        class MyEntity(SequinEntity):
            pass

        self.assertEqual(MyEntity().name(), 'myentity')

    def test_create_entity(self):
        class MyEntity(SequinEntity):
            def reduce(self, event):
                if event.action == 'create':
                    self.data['initialized'] = True
                    self.data['count'] = 0
                elif event.action == 'increment':
                    self.data['count'] += json.loads(event.content)
                elif event.action == 'decrement':
                    self.data['count'] -= json.loads(event.content)

            def increment(self, count):
                return self.create_mutate_event('increment', json.dumps(count))

            def decrement(self, count):
                return self.create_mutate_event('decrement', json.dumps(count))

        db = SqliteDatabase(':memory:')

        register_database(db)
        SequinEvent.create_table()

        m = MyEntity.create_new('foobar')
        self.assertEqual(m.uid, 'foobar')

    def test_basic_events(self):
        class MyEntity(SequinEntity):
            def reduce(self, event):
                if event.action == 'create':
                    self.data['initialized'] = True
                    self.data['count'] = 0
                elif event.action == 'increment':
                    self.data['count'] += json.loads(event.content)
                elif event.action == 'decrement':
                    self.data['count'] -= json.loads(event.content)

            def increment(self, count):
                return self.create_mutate_event('increment', json.dumps(count))

            def decrement(self, count):
                return self.create_mutate_event('decrement', json.dumps(count))

        db = SqliteDatabase(':memory:')

        register_database(db)
        SequinEvent.create_table()

        m = MyEntity.create_new('foobar')
        m.increment(5)
        m.decrement(4)
        self.assertEqual(m.data['count'], 1)

    def test_reconstruction(self):
        class MyEntity(SequinEntity):
            def reduce(self, event):
                if event.action == 'create':
                    self.data['initialized'] = True
                    self.data['count'] = 0
                elif event.action == 'increment':
                    self.data['count'] += json.loads(event.content)
                elif event.action == 'decrement':
                    self.data['count'] -= json.loads(event.content)

            def increment(self, count):
                return self.create_mutate_event('increment', json.dumps(count))

            def decrement(self, count):
                return self.create_mutate_event('decrement', json.dumps(count))

        db = SqliteDatabase(':memory:')

        register_database(db)
        SequinEvent.create_table()

        m = MyEntity.create_new('foobar')
        m.increment(5)
        m.decrement(4)

        m2 = MyEntity.get('foobar')

        self.assertEqual(m.data, m2.data)

    def test_long_lifespan_reconstruction(self):
        class MyEntity(SequinEntity):
            def reduce(self, event):
                if event.action == 'create':
                    self.data['initialized'] = True
                    self.data['count'] = 0
                elif event.action == 'increment':
                    self.data['count'] += json.loads(event.content)
                elif event.action == 'decrement':
                    self.data['count'] -= json.loads(event.content)

            def increment(self, count):
                return self.create_mutate_event('increment', json.dumps(count))

            def decrement(self, count):
                return self.create_mutate_event('decrement', json.dumps(count))

        db = SqliteDatabase(':memory:')

        register_database(db)
        SequinEvent.create_table()

        m = MyEntity.create_new('foobar')
        import random

        aggregate = 0

        for i in range(0, 99):
            value = random.randint(0, 99)
            if i % 3 != 0:
                aggregate += value
                m.increment(value)
            else:
                aggregate -= value
                m.decrement(value)

        self.assertEqual(m.data['count'], aggregate)

        m2 = MyEntity.get('foobar')
        self.assertEqual(m2.data['count'], aggregate)

    def test_stale_mutation(self):
        class MyEntity(SequinEntity):
            def reduce(self, event):
                if event.action == 'create':
                    self.data['initialized'] = True
                    self.data['count'] = 0
                elif event.action == 'increment':
                    self.data['count'] += json.loads(event.content)
                elif event.action == 'decrement':
                    self.data['count'] -= json.loads(event.content)

            def increment(self, count, **kwargs):
                return self.create_mutate_event('increment', json.dumps(count), **kwargs)

            def decrement(self, count, **kwargs):
                return self.create_mutate_event('decrement', json.dumps(count), **kwargs)

        db = SqliteDatabase(':memory:')

        register_database(db)
        SequinEvent.create_table()

        m = MyEntity.create_new('foobar')
        m.increment(2, commit=False)

        print m.is_current()

        self.assertRaises(EntityStaleError, m.increment, 1)

    def test_lock_contention(self):
        class MyEntity(SequinEntity):
            def reduce(self, event):
                if event.action == 'create':
                    self.data['initialized'] = True
                    self.data['count'] = 0
                elif event.action == 'increment':
                    self.data['count'] += json.loads(event.content)
                elif event.action == 'decrement':
                    self.data['count'] -= json.loads(event.content)

            def increment(self, count, **kwargs):
                return self.create_mutate_event('increment', json.dumps(count), **kwargs)

            def decrement(self, count, **kwargs):
                return self.create_mutate_event('decrement', json.dumps(count), **kwargs)

        db = SqliteDatabase(':memory:')

        if isinstance(db, SqliteDatabase):
            self.skipTest('Skipping lock contention test on SQLite')

        # Test only functional on non-SQLite DBs
        register_database(db)
        SequinEvent.create_table()

        db.set_autocommit(False)

        m = MyEntity.create_new('foobar')
        m.increment(2)

        self.assertRaises(EntityStaleError, m.increment, 1)


# End
