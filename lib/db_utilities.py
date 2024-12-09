import copy
import os
from functools import partial

from sqlmodel import Session, SQLModel, create_engine

from lib.db_datamodel import Attr, CxnDef, CxnOcc, Group, Model, ObjDef, ObjOcc


def create_group(group):
    attrs = [
        Attr(name=name, value=value) for name, value in group.get("attrs", {}).items()
    ]

    return Group(
        aris_id=group["aris_id"],
        guid=group["guid"],
        name=group["name"],
        level=group["level"],
        attrs=attrs,
        path=group["path"],
    )


def create_cxn_def(cxn_def):
    attrs = [
        Attr(name=name, value=value) for name, value in cxn_def.get("attrs", {}).items()
    ]

    return CxnDef(
        aris_id=cxn_def["aris_id"],
        guid=cxn_def["guid"],
        type=cxn_def["type"],
        attrs=attrs,
    )


def create_obj_def(db_data, obj_def):
    attrs = [
        Attr(name=name, value=value) for name, value in obj_def.get("attrs", {}).items()
    ]

    cxns = [db_data[cxn_id] for cxn_id in obj_def.get("cxns", [])]

    return ObjDef(
        aris_id=obj_def["aris_id"],
        parent=db_data[obj_def["parent"]],
        guid=obj_def["guid"],
        name=obj_def["name"],
        type=obj_def["type"],
        symbol=obj_def["symbol"],
        cxns=cxns,
        attrs=attrs,
        path=obj_def["path"],
    )


def create_cxn_occ(db_data, cxn_occ):
    return CxnOcc(
        aris_id=cxn_occ["aris_id"],
        cxn_def=db_data[cxn_occ["cxn_def"]],
    )


def create_obj_occ(db_data, obj_occ):
    return ObjOcc(
        aris_id=obj_occ["aris_id"],
        symbol=obj_occ["symbol"],
        derived_symbol=obj_occ.get("derived_symbol"),
        x=obj_occ["x"],
        y=obj_occ["y"],
        obj_def=db_data[obj_occ["obj_def"]],
        model=db_data[obj_occ["model_id"]],
        cxns=[db_data[cxn_id] for cxn_id in obj_occ.get("cxns", [])],
    )


def create_model(db_data, model):
    attrs = [
        Attr(name=name, value=value) for name, value in model.get("attrs", {}).items()
    ]

    db_model = Model(
        aris_id=model["aris_id"],
        parent=db_data[model["parent"]],
        guid=model["guid"],
        name=model["name"],
        type=model["type"],
        attrs=attrs,
        path=model["path"],
    )

    return db_model


def link_group_parent(db_data, data):
    for group_id, group in data["groups"].items():
        db_data[group_id].parent = db_data.get(group["parent"])


def link_cxn_defs_to_obj_defs(db_data, data):
    for cxn_def_id, cxn_def in data["cxn_defs"].items():
        db_cxn_def = db_data[cxn_def_id]

        connected_to = cxn_def.get("connected_to")
        if connected_to:
            db_cxn_def.connected_to = db_data[connected_to]


def link_cxn_occs_to_obj_occs(db_data, data):
    for cxn_occ_id, cxn_occ in data["cxn_occs"].items():
        db_cxn_occ = db_data[cxn_occ_id]

        connected_to = cxn_occ.get("connected_to")
        if connected_to:
            db_cxn_occ.connected_to = db_data[connected_to]


def link_superior_defs_to_models(db_data, data):
    for obj_def_id, model_ids in data["def_to_models"].items():
        if len(model_ids) > 0:
            db_obj_def = db_data[obj_def_id]
            db_obj_def.linked_models = [db_data[model_id] for model_id in model_ids]


def add_obj_occs_to_model(db_data, data):
    for model_id, model in data["models"].items():
        db_data[model_id].occs = [db_data[occ_id] for occ_id in model.get("occs", [])]


def create_database(data, sqlite_filename):
    sqlite_url = f"sqlite:///{sqlite_filename}"

    if os.path.exists(sqlite_filename):
        os.remove(sqlite_filename)

    engine = create_engine(sqlite_url, echo=False)
    SQLModel.metadata.create_all(engine)

    db_data = {}

    type_and_func = (
        ("groups", create_group),
        ("cxn_defs", create_cxn_def),
        ("obj_defs", partial(create_obj_def, db_data)),
        ("models", partial(create_model, db_data)),
        ("cxn_occs", partial(create_cxn_occ, db_data)),
        ("obj_occs", partial(create_obj_occ, db_data)),
    )

    with Session(engine) as session:
        for aris_type, func in type_and_func:
            for item in data[aris_type].values():
                db_data[item["aris_id"]] = func(item)

                if aris_type == "models":
                    session.add(db_data[item["aris_id"]])

        link_group_parent(db_data, data)
        link_cxn_defs_to_obj_defs(db_data, data)
        link_cxn_occs_to_obj_occs(db_data, data)
        add_obj_occs_to_model(db_data, data)
        link_superior_defs_to_models(db_data, data)

        session.commit()
