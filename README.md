# aml_parser
## Utility library to parse and query ARIS AML exported XML files.

The AMLQuery class provides convenience methods for querying the data. `lib/parser.py` can be used by itself to parse the XML and create the database entries.

### Usage:
All the database objects support code completion and they have child/parent relationship attributes to make working with them more Pythonic.

```
from sqlmodel import Session

from aml_query import AMLQuery

aml_query = AMLQuery("ARIS_AML_Export.xml", force_parse=False)


with Session(aml_query.engine) as session:
    for model in aml_query.get_models(session):
        print(model.name)
        for occ in model.occs:
            print(occ.symbol)
```

