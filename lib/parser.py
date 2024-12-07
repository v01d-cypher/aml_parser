import os.path
import re

import lxml.etree as ET

from lib.db_utilities import create_database


class AMLParser:
    """
    Parse Aris XML and store data in a SQLite database
    """

    def __init__(self, aml_filename):
        self.groups = {}
        self.obj_defs = {}
        self.cxn_defs = {}
        self.models = {}
        self.def_to_models = {}
        self.path = []
        self.parse_aml(aml_filename)

    def parse_aml(self, aml_filename):
        print(f"Parsing AML file '{aml_filename}' ...")

        context = ET.iterparse(aml_filename, ("start", "end"))
        self.low_memory_iter(context)

        data = {
            "groups": self.groups,
            "obj_defs": self.obj_defs,
            "cxn_defs": self.cxn_defs,
            "models": self.models,
            "def_to_models": self.def_to_models,
        }

        sqlite_filename = f"{os.path.splitext(aml_filename)[0]}.db"
        print(f"Creating SQLite Database '{sqlite_filename}' ...")
        create_database(data, sqlite_filename)

    def parse_attr_defs(self, item):
        attrs = {}

        attr_defs = item.findall("AttrDef")
        for attr in attr_defs:
            plain_text = attr.findall(".//PlainText")

            attr_text = " ".join(
                (text.get("TextValue") for text in plain_text if text.get("TextValue"))
            )

            if attr_text:
                attrs[attr.get("AttrDef.Type")] = attr_text.strip()
            else:
                attr_value = attr.find(".//AttrValue")
                if attr_value is not None and attr_value.text is not None:
                    attrs[attr.get("AttrDef.Type")] = attr_value.text.strip()
                else:
                    attrs[attr.get("AttrDef.Type")] = ""

        name = None
        if "AT_NAME" in attrs:
            name = attrs.get("AT_NAME", "")
            del attrs["AT_NAME"]

        return name, attrs

    def parse_cxn_occs(self, item):
        occ_cxns = {}

        cxn_occs = item.findall("CxnOcc")
        for cxn in cxn_occs:
            id = cxn.get("CxnOcc.ID")
            occ_cxns[id] = {
                "aris_id": id,
                "cxn_def": cxn.get("CxnDef.IdRef"),
                "connected_to": cxn.get("ToObjOcc.IdRef"),
            }

        return occ_cxns

    def parse_obj_occs(self, item):
        occs = {}

        obj_occs = item.findall("ObjOcc")
        for occ in obj_occs:
            id = occ.get("ObjOcc.ID")
            symbol_GUID = occ.find("SymbolGUID")
            position = occ.find("Position").attrib

            new_values = {
                "aris_id": id,
                "model_id": item.get("Model.ID"),
                "obj_def": occ.get("ObjDef.IdRef", ""),
                "x": int(position.get("Pos.X", "0")),
                "y": int(position.get("Pos.Y", "0")),
                "cxns": self.parse_cxn_occs(occ),
            }

            if symbol_GUID is not None:
                new_values["symbol"] = symbol_GUID.text
                new_values["derived_symbol"] = occ.get("SymbolNum", "")
            else:
                new_values["symbol"] = occ.get("SymbolNum", "")

            occs.setdefault(id, {}).update(new_values)

        return occs

    def parse_cxn_defs(self, item):
        obj_cxns = {}

        cxn_defs = item.findall("CxnDef")
        for cxn in cxn_defs:
            id = cxn.get("CxnDef.ID")
            _, attrs = self.parse_attr_defs(cxn)

            obj_cxns[id] = {
                "aris_id": id,
                "guid": cxn.find("GUID").text,
                "type": cxn.get("CxnDef.Type"),
                "connected_to": cxn.get("ToObjDef.IdRef"),
                "attrs": attrs,
            }

            self.cxn_defs.update(obj_cxns)

        return obj_cxns

    def parse_obj_defs(self, item):
        obj_defs = item.findall("ObjDef")
        for obj in obj_defs:
            id = obj.get("ObjDef.ID")
            linked_models = (
                re.sub(r"\s+", " ", obj.get("LinkedModels.IdRefs", "")).strip().split()
            )
            name, attrs = self.parse_attr_defs(obj)
            def_cxns = self.parse_cxn_defs(obj)

            new_values = {
                "aris_id": id,
                "parent": obj.getparent().get("Group.ID"),
                "guid": obj.find("GUID").text,
                "name": name,
                "type": obj.get("TypeNum", ""),
                "symbol": obj.get("SymbolNum", ""),
                "linked_models": linked_models,
                "cxns": list(def_cxns.keys()),
                "attrs": attrs,
                "path": "/".join(self.path),
            }

            self.obj_defs.setdefault(id, {}).update(new_values)

            self.def_to_models.setdefault(id, []).extend(linked_models)

    def parse_models(self, item):
        obj_models = item.findall("Model")
        for model in obj_models:
            id = model.get("Model.ID")
            name, attrs = self.parse_attr_defs(model)

            self.models[id] = {
                "aris_id": id,
                "parent": model.getparent().get("Group.ID"),
                "guid": model.find("GUID").text,
                "name": name,
                "type": model.get("Model.Type"),
                "occs": self.parse_obj_occs(model),
                "attrs": attrs,
                "path": "/".join(self.path),
            }

    def low_memory_iter(self, context):
        for event, element in context:
            if (
                event == "start"
                and element.tag == "Group"
                and element.get("Group.ID") == "Group.Root"
            ):
                _, attrs = self.parse_attr_defs(element)
                self.path.append(".")

                self.groups["Group.Root"] = {
                    "aris_id": "Group.Root",
                    "guid": None,
                    "name": ".",
                    "parent": None,
                    "level": 0,
                    "attrs": attrs,
                    "path": ".",
                }

            elif event == "start" and element.tag == "Group":
                parent = element.getparent().get("Group.ID")

                name, attrs = self.parse_attr_defs(element)
                self.path.append(name)

                group_id = element.get("Group.ID")
                self.groups[group_id] = {
                    "aris_id": group_id,
                    "guid": element.find("GUID").text,
                    "name": name,
                    "parent": parent,
                    "level": len(self.path) - 1,
                    "attrs": attrs,
                    "path": "/".join(self.path),
                }

            elif event == "end" and element.tag == "Group":
                self.parse_obj_defs(element)

                self.parse_models(element)

                if len(self.path) > 1:
                    self.path.pop()

                element.clear()

                for ancestor in element.xpath("ancestor-or-self::*"):
                    if ancestor.tag == "AML":
                        continue

                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]
        del context
