import os.path

from sqlalchemy import func
from sqlmodel import Session, col, create_engine, or_, select

from lib.db_datamodel import CxnDef, CxnOcc, Group, Model, ObjDef, ObjOcc
from lib.parser import AMLParser


class AMLQuery:
    def __init__(self, aml_filename: str, force_parse: bool = False):
        if not os.path.exists(aml_filename):
            raise SystemExit(f"Error: No valid AML filename provided.")

        sqlite_filename = f"{os.path.splitext(aml_filename)[0]}.db"
        if not os.path.exists(sqlite_filename) or force_parse:
            AMLParser(aml_filename)

        if not os.path.exists(sqlite_filename):
            raise SystemExit(f"Error: Could not create database {sqlite_filename}.")

        sqlite_url = f"sqlite:///{sqlite_filename}"
        self.engine = create_engine(sqlite_url, echo=False)
        print(f"Opened '{sqlite_filename}'.")

    def get_assigned_fad(self, item: ObjDef | ObjOcc) -> Model | None:
        """
        Returns the FAD for this object definition
        (or the occurance's object definition).
        """

        if item.aris_type == "ObjDef":
            obj_def = item
        else:
            obj_def = item.obj_def

        linked_FADs = [
            model
            for model in obj_def.linked_models
            if model.type == "MT_FUNC_ALLOC_DGM"
        ]
        if linked_FADs:
            return linked_FADs[0]

    def get_connected_occs(
        self,
        obj_occ: ObjOcc,
        symbol_types: list[str] | str = None,
        cxn_types: list[str] | str = None,
    ) -> list[ObjOcc]:
        """
        Return connected occurances, optionally filtered by connection type and/or symbol.
        """

        if symbol_types is not None:
            symbol_types = (
                symbol_types if isinstance(symbol_types, list) else [symbol_types]
            )

        if cxn_types is not None:
            cxn_types = cxn_types if isinstance(cxn_types, list) else [cxn_types]

        connected_occs = [
            cxn.connected_to
            for cxn in obj_occ.cxns
            if (cxn.connected_to is not None)
            and (cxn.type in cxn_types if cxn_types is not None else True)
            and (
                cxn.connected_to.symbol in symbol_types
                if symbol_types is not None
                else True
            )
        ]

        for occ in obj_occ.model.occs:
            for cxn in occ.cxns:
                if (
                    cxn.connected_to == obj_occ
                    and (cxn.type in cxn_types if cxn_types is not None else True)
                    and (
                        occ.symbol in symbol_types if symbol_types is not None else True
                    )
                ):
                    connected_occs.append(occ)

        return connected_occs

    def find_model(
        self,
        session: Session,
        aris_id: str = None,
        guid: str = None,
    ) -> Model | None:
        """
        Retrieve a model matching either guid or aris_id
        """

        statement = select(Model).where(
            or_(Model.guid == guid, Model.aris_id == aris_id)
        )

        return session.exec(statement).one_or_none()

    def get_models(
        self,
        session: Session,
        model_types: list[str] | str = None,
    ) -> list[Model]:
        """
        Retrieve models, optionally filtered by type.
        """

        if model_types is not None:
            model_types = (
                model_types if isinstance(model_types, list) else [model_types]
            )
            statement = select(Model).where(Model.type.in_(model_types))
        else:
            statement = select(Model)

        return session.exec(statement)

    def filter_occs_by_symbol(
        self,
        item: Model | list[ObjOcc],
        symbol_types: list[str] | str,
    ):
        """
        Return occurances filtered by symbol.
        """

        if isinstance(item, list):
            occs = item
        else:
            occs = item.occs

        symbol_types = (
            symbol_types if isinstance(symbol_types, list) else [symbol_types]
        )

        return [occ for occ in occs if occ.symbol in symbol_types]

    def db_stats(self, session: Session) -> dict:
        stats = {
            "groups": Group,
            "cxn_defs": CxnDef,
            "obj_defs": ObjDef,
            "cxn_occs": CxnOcc,
            "obj_occ": ObjOcc,
            "models": Model,
        }
        for obj_type, db_type in stats.items():
            stats[obj_type] = session.exec(select(func.count(col(db_type.id)))).one()

        return stats
