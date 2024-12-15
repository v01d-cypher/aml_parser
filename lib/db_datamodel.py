from pydantic import ConfigDict
from sqlmodel import Field, Relationship, SQLModel


class Attr(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str
    value: str
    group_id: int | None = Field(default=None, foreign_key="group.id")
    cxn_def_id: int | None = Field(default=None, foreign_key="cxndef.id")
    obj_def_id: int | None = Field(default=None, foreign_key="objdef.id")
    model_id: int | None = Field(default=None, foreign_key="model.id")

    # Because we want to use model_id
    model_config = ConfigDict(protected_namespaces=("protect_ns_",))

    def __repr__(self) -> str:
        return f"{self.name}={self.value}"


class AttrMixin:
    def get_attr(self, attr_name):
        for attr in self.attrs:
            if attr.name == attr_name:
                return attr.value

    def attrs_to_dict(self):
        return {attr.name: attr.value for attr in self.attrs}


class Group(SQLModel, AttrMixin, table=True):
    id: int = Field(default=None, primary_key=True)
    aris_id: str
    guid: str | None
    name: str
    level: int
    path: str

    parent_id: int | None = Field(default=None, foreign_key="group.id")
    parent: "Group" = Relationship(
        back_populates="groups", sa_relationship_kwargs={"remote_side": "Group.id"}
    )

    groups: list["Group"] | None = Relationship(back_populates="parent")
    obj_defs: list["ObjDef"] | None = Relationship(back_populates="parent")
    models: list["Model"] | None = Relationship(back_populates="parent")

    attrs: list[Attr] | None = Relationship()
    aris_type: str = Field(default="Group")

    @property
    def hasChildren(self) -> bool:
        """
        Returns True if this Group has any groups or models as children.
        """
        # Although Groups contain Object Definitions, for our reporting purposes they
        # don't count as children
        return (len(self.models) + len(self.groups)) > 0


class CxnDef(SQLModel, AttrMixin, table=True):
    id: int = Field(default=None, primary_key=True)
    aris_id: str
    guid: str
    type: str

    connected_to_id: int = Field(foreign_key="objdef.id")
    connected_to: "ObjDef" = Relationship(
        sa_relationship_kwargs={"foreign_keys": "CxnDef.connected_to_id"}
    )

    obj_def_id: int = Field(foreign_key="objdef.id")
    occs: list["CxnOcc"] = Relationship(back_populates="cxn_def")

    attrs: list[Attr] | None = Relationship()
    aris_type: str = Field(default="CxnDef")


class ObjDef(SQLModel, AttrMixin, table=True):
    id: int = Field(default=None, primary_key=True)
    aris_id: str
    guid: str
    name: str
    type: str
    symbol: str
    path: str

    parent_id: int = Field(foreign_key="group.id")
    parent: Group = Relationship(back_populates="obj_defs")

    linked_models: list["Model"] | None = Relationship(back_populates="superior_def")

    cxns: list[CxnDef] | None = Relationship(
        sa_relationship_kwargs={"foreign_keys": "CxnDef.obj_def_id"}
    )
    attrs: list[Attr] | None = Relationship()

    occs: list["ObjOcc"] | None = Relationship(back_populates="obj_def")

    aris_type: str = Field(default="ObjDef")


class Model(SQLModel, AttrMixin, table=True):
    id: int = Field(default=None, primary_key=True)
    aris_id: str
    guid: str
    name: str
    type: str
    path: str

    seperior_def_id: int | None = Field(foreign_key="objdef.id")
    superior_def: ObjDef | None = Relationship(back_populates="linked_models")

    parent_id: int = Field(foreign_key="group.id")
    parent: Group = Relationship(back_populates="models")

    occs: list["ObjOcc"] | None = Relationship(back_populates="model")
    attrs: list[Attr] | None = Relationship()

    aris_type: str = Field(default="Model")


class CxnOcc(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    aris_id: str

    cxn_def_id: int = Field(foreign_key="cxndef.id")
    cxn_def: CxnDef = Relationship(back_populates="occs")

    connected_to_id: int = Field(foreign_key="objocc.id")
    connected_to: "ObjOcc" = Relationship(
        sa_relationship_kwargs={"foreign_keys": "CxnOcc.connected_to_id"}
    )

    obj_occ_id: int = Field(foreign_key="objocc.id")
    aris_type: str = Field(default="CxnOcc")

    @property
    def type(self) -> str:
        """
        Returns the type of the occurance from the connection definition.
        """
        return self.cxn_def.type


class ObjOcc(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    aris_id: str
    symbol: str
    derived_symbol: str | None = Field(default=None)
    x: int = Field(default=0)
    y: int = Field(default=0)
    width: int = Field(default=0)
    height: int = Field(default=0)

    obj_def_id: int = Field(foreign_key="objdef.id")
    obj_def: ObjDef = Relationship(back_populates="occs")

    model_id: int = Field(foreign_key="model.id")
    model: Model = Relationship(back_populates="occs")

    cxns: list[CxnOcc] | None = Relationship(
        sa_relationship_kwargs={"foreign_keys": "CxnOcc.obj_occ_id"}
    )

    aris_type: str = Field(default="ObjOcc")

    # Because we want to use model_id
    model_config = ConfigDict(protected_namespaces=("protect_ns_",))

    @property
    def name(self) -> str:
        """
        Returns the name of the occurance from the object definition.
        """
        return self.obj_def.name
