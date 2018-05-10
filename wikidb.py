import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.types import String, Integer, Text
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import func

class DB:
    #db_path = 'sqlite:///wiki.db'
    db_path = 'postgresql://postgres:nik1ita@localhost/mydb'

    def __init__(self):
        self._engine = engine = create_engine(self.db_path)
        self._engine.execution_options(stream_results=True)
        self.session = engine.connect()
        self.md = MetaData(bind=self.session)
        self._table = Table('page', self.md, 
                        Column('id', String(20), primary_key=True),
                        Column('title', Text()), 
                        Column('text', Text()),
                        Column('timestamp', String(50))               
                       )
        
    def recreate(self):
        self.md.drop_all()
        self.md.create_all()

    def exist(self, obj_id):
        query = select([self._table.c.id,]).where(self._table.c.id == obj_id)
        data = self.session.execute(query)
        return len(list(data)) > 0

        return self.session.scalar(self._table.select([func.count('*')]).where(self._table.c.id == obj_id)) > 0
        #return self.session.scalar(self._table.select([func.count('*')]).where(self._table.c.id == obj_id)) > 0
        
    def insert(self, data):
        if not self.exist(data['id']):
            return self.session.execute(self._table.insert().values(**data))
        else:
            return True
    
    def select(self, obj_id=None, limit=None, chunks=None):
        if obj_id is None:
            query = self._table.select()
        else:
            query = self._table.select().where(self._table.c.id == obj_id)

        if limit is not None:
            query = query.limit(limit)

       #if chunks is not None:
       #    query = query.yield_per(chunks)
            
        return self.session.execution_options(stream_results=True).execute(query)

    def count(self):
        return self.session.scalar(select([func.count('*')]).select_from(self._table))

    @staticmethod
    def test():
        sample_page = {
            'id': 12,
            'title': 'test',
            'text': 'hkvbnldfkjvbqerbvqeklbvqebvlqelbvqelbv',
            'timestamp': '2017-08-15T05:02:43Z',    
        }

        db = DB()
        db.recreate()

        for i in range(10):
            sample_page['id'] = str(i)
            db.insert(sample_page)

        print("select id = 5")
        for i in db.select(obj_id=5):
            print(dict(i))
            
        print("select *")
        for i in db.select():
            print(dict(i))

        db.recreate()

if __name__ == '__main__':
    print(DB().count())    
   #for i in DB().select(limit=10):
   #    print(dict(i))
    #DB.test()
