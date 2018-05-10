from utils import get_page_url
from wikidb import DB

db = DB()
doc_id = input()
print(get_page_url(doc_id))
print(db.select(obj_id=str(doc_id)).fetchone())
