import datetime

import shortuuid
from peewee import *
from playhouse.shortcuts import model_to_dict

from errors import EntityAlreadyExistsError, EntityStaleError

database_proxy = Proxy()


def register_database(db):
    database_proxy.initialize(db)


class SequinEvent(Model):
    eid = PrimaryKeyField()

    stamp = DateTimeField(default=datetime.datetime.now)

    entity = TextField(index=True, null=False)
    uid = TextField(index=True, unique=False, null=False)

    action = TextField(index=True, null=False)
    content = TextField(index=True, null=True)

    class Meta:
        database = database_proxy

    def save(self, **kwargs):
        if self.eid:
            raise Exception('Cannot mutate existing event')
        else:
            super(SequinEvent, self).save(**kwargs)


class SequinEntity(object):
    def __init__(self):
        self.created_at = None
        self.updated_at = None
        self.last_event = None

        self.uid = None
        self.data = {}

    @classmethod
    def name(cls):
        return cls.__name__.lower()

    @classmethod
    def create_new(cls, uid=None):
        if uid is None:
            uid = shortuuid.uuid()
        event, created = SequinEvent.get_or_create(entity=cls.name(), action='create', uid=uid)
        if created:
            e = cls()
            e.commit(event)
            return e
        raise EntityAlreadyExistsError('Entity {}:{} already exists'.format(cls.name(), uid))

    @classmethod
    def get(cls, uid):
        events = SequinEvent.select().where(SequinEvent.uid == uid).order_by(SequinEvent.eid.asc())
        return cls.compose(events)

    def is_current(self):
        return SequinEvent.select().where(SequinEvent.entity == self.name(), SequinEvent.uid == self.uid,
                                   SequinEvent.eid > self.last_event['eid']).count() == 0

    def create_mutate_event(self, action, content, commit=True):
        event = None
        with SequinEvent._meta.database.atomic() as txn:
            try:
                # print 'Locking on SequinEvent domain for {}:{}'.format(self.name(), self.uid)
                if SequinEvent._meta.database.for_update:
                    lock_query = SequinEvent.select().for_update(nowait=True).where(SequinEvent.entity == self.name(), SequinEvent.uid == self.uid)
                    lock_query.execute()

                if not self.is_current():
                    raise EntityStaleError('Entity {}:{} is stale.'.format(self.name(), self.uid))

                event = SequinEvent.create(entity=self.name(), action=action, uid=self.uid, content=content)
            except EntityStaleError:
                raise
            except OperationalError:
                raise EntityStaleError('Entity {}:{} is stale.'.format(self.name(), self.uid))
            except Exception as e:
                print e
                return None

        # print 'Successfully stored new SequinEvent.'
        if commit:
            self.commit(event)
        return event

    @classmethod
    def compose(cls, events):
        entity = cls()
        for e in events:
            entity.commit(e)
        return entity

    def commit(self, event):
        if self.created_at is None:
            self.created_at = event.stamp
            self.uid = event.uid

        self.updated_at = event.stamp
        self.last_event = model_to_dict(event)
        self.reduce(event)

    def reduce(self, event):
        print 'Override me and don\'t bother calling super!'
        pass

# End
