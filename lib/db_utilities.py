import copy
import os

from sqlmodel import Session, SQLModel, create_engine

from lib.db_datamodel import Attr, CxnDef, CxnOcc, Group, Model, ObjDef, ObjOcc


def create_group(group, data):
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


def create_cxn_def(cxn_def, data):
    attrs = [
        Attr(name=name, value=value) for name, value in cxn_def.get("attrs", {}).items()
    ]

    return CxnDef(
        aris_id=cxn_def["aris_id"],
        guid=cxn_def["guid"],
        type=cxn_def["type"],
        attrs=attrs,
    )


def create_obj_def(obj_def, data):
    attrs = [
        Attr(name=name, value=value) for name, value in obj_def.get("attrs", {}).items()
    ]

    cxns = [data["cxn_defs"][cxn_id] for cxn_id in obj_def.get("cxns", [])]

    return ObjDef(
        aris_id=obj_def["aris_id"],
        parent=data["groups"][obj_def["parent"]],
        guid=obj_def["guid"],
        name=obj_def["name"],
        type=obj_def["type"],
        symbol=obj_def["symbol"],
        cxns=cxns,
        attrs=attrs,
        path=obj_def["path"],
    )


def create_cxn_occ(cxn_occ, db_obj_occs, data):
    return CxnOcc(
        aris_id=cxn_occ["aris_id"],
        cxn_def=data["cxn_defs"][cxn_occ["cxn_def"]],
        connected_to=db_obj_occs.get(cxn_occ.get("connected_to")),
    )


def create_obj_occ(obj_occ, model, data):
    return ObjOcc(
        aris_id=obj_occ["aris_id"],
        symbol=obj_occ["symbol"],
        derived_symbol=obj_occ.get("derived_symbol"),
        x=obj_occ["x"],
        y=obj_occ["y"],
        obj_def=data["obj_defs"][obj_occ["obj_def"]],
        model=model,
    )


def create_model(model, data):
    attrs = [
        Attr(name=name, value=value) for name, value in model.get("attrs", {}).items()
    ]

    db_model = Model(
        aris_id=model["aris_id"],
        parent=data["groups"][model["parent"]],
        guid=model["guid"],
        name=model["name"],
        type=model["type"],
        attrs=attrs,
        path=model["path"],
    )

    # First create all obj_occs as cxn_occs need to be connected to them
    db_obj_occs = {
        obj_occ_id: create_obj_occ(obj_occ, db_model, data)
        for obj_occ_id, obj_occ in model.get("occs", {}).items()
    }

    for obj_occ_id, obj_occ in model.get("occs", {}).items():
        db_obj_occ = db_obj_occs[obj_occ_id]
        db_obj_occ.cxns = [
            create_cxn_occ(cxn_occ, db_obj_occs, data)
            for cxn_occ in obj_occ.get("cxns", {}).values()
        ]

    db_model.occs = list(db_obj_occs.values())

    return db_model


def link_cxn_defs_to_obj_defs(data):
    for cxn_def_id, cxn_def in data["org_cxn_defs"].items():
        db_cxn_def = data["cxn_defs"][cxn_def_id]

        connected_to = cxn_def.get("connected_to")
        if connected_to:
            db_cxn_def.connected_to = data["obj_defs"][connected_to]


def link_superior_defs_to_models(data):
    for obj_def_id, model_ids in data["def_to_models"].items():
        if len(model_ids) > 0:
            db_obj_def = data["obj_defs"][obj_def_id]
            db_obj_def.linked_models = [
                data["models"][model_id] for model_id in model_ids
            ]


def link_group_parent(data):
    for group_id, db_group in data["groups"].items():
        db_group.parent = data["groups"].get(data["org_groups"][group_id]["parent"])


def create_database(data, sqlite_filename):
    sqlite_url = f"sqlite:///{sqlite_filename}"

    if os.path.exists(sqlite_filename):
        os.remove(sqlite_filename)

    engine = create_engine(sqlite_url, echo=False)
    SQLModel.metadata.create_all(engine)

    data["org_cxn_defs"] = copy.deepcopy(data["cxn_defs"])
    data["org_groups"] = copy.deepcopy(data["groups"])

    type_and_func = (
        ("groups", create_group),
        ("cxn_defs", create_cxn_def),
        ("obj_defs", create_obj_def),
        ("models", create_model),
    )

    with Session(engine) as session:
        for aris_type, func in type_and_func:
            for item in data[aris_type].values():
                db_item = func(item, data)
                data[aris_type][item["aris_id"]] = db_item

                if aris_type == "models":
                    session.add(db_item)

        link_group_parent(data)
        link_cxn_defs_to_obj_defs(data)
        link_superior_defs_to_models(data)

        session.commit()
