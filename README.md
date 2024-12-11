# aml_parser
## Utility library to parse and query ARIS AML exported XML files.

The AMLQuery class provides convenience methods for querying the data. Alternatively, `lib/parser.py` can be used by itself to parse the XML and create the database entries.

All the database objects support code completion and they have child/parent relationship attributes to make working with them more Pythonic.

### Usage:
```
from aml_query import AMLQuery

aml_query = AMLQuery("ARIS_AML_Export.xml", force_parse=False)


print(aml_query.db_stats)

for model in aml_query.get_models():
    print(model.name)

    for occ in model.occs:
        print(occ.symbol)
        print(occ.obj_def.name)
    
    # Dump db model to JSON file
    with open(f"model.json", "w") as f:
        f.write(model.model_dump_json(indent=2))
```


#### With your own session:
```
from sqlmodel import Session, select

from aml_query import AMLQuery
from lib.db_datamodel import Model

aml_query = AMLQuery("ARIS_AML_Export.xml", force_parse=False)


with Session(aml_query.engine) as session:
    query = select(Model).where(Model.type == "MT_FUNC_ALLOC_DGM")

    for model in session.exec(query):
        print(model.name)
```