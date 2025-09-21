import spacy
import re

nlp = spacy.load("en_core_web_sm")

def parse_natural_conditions(text, fields):
    text = text.lower()
    if " and " in text:
        parts = text.split(" and ")
        return {"$and": [parse_natural_conditions(p.strip(), fields) for p in parts]}
    elif " or " in text:
        parts = text.split(" or ")
        return {"$or": [parse_natural_conditions(p.strip(), fields) for p in parts]}
    else:
        pattern = r"(\w+)\s*(>=|<=|>|<|=|is)\s*([\w\d]+)"
        match = re.match(pattern, text)
        if not match:
            return {}
        field, op, val = match.groups()
        if field not in fields:
            return {}

        field_type = fields[field]
        if field_type in ("int", "float"):
            try:
                val = float(val)
                if val.is_integer():
                    val = int(val)
            except:
                pass

        if op == ">" or op == ">=":
            mongo_op = "$gte" if op == ">=" else "$gt"
            return {field: {mongo_op: val}}
        elif op == "<" or op == "<=":
            mongo_op = "$lte" if op == "<=" else "$lt"
            return {field: {mongo_op: val}}
        elif op in ("=", "is"):
            return {field: val}
        return {}

def parse_switch_expression(text, field):
    pattern = r"if (\w+) (>|<|=|>=|<=) (\d+) then '(\w+)' else '(\w+)'"
    match = re.search(pattern, text.lower())
    if not match:
        return None
    cond_field, op, val, then_val, else_val = match.groups()
    if cond_field != field:
        return None

    branch_operator_map = {
        ">": "$gt",
        "<": "$lt",
        "=": "$eq",
        ">=": "$gte",
        "<=": "$lte"
    }
    mongo_op = branch_operator_map[op]

    switch_expr = {
        "$switch": {
            "branches": [
                {
                    "case": {mongo_op: [f"${field}", int(val)]},
                    "then": then_val
                }
            ],
            "default": else_val
        }
    }
    return switch_expr

def parse_filter_expression(array_field, cond_text, fields):
    if "where" not in cond_text:
        return None
    _, cond = cond_text.split("where", 1)
    cond = cond.strip()
    condition = parse_natural_conditions(cond, fields)
    if not condition:
        return None

    def replace_field_refs(cond_obj):
        if isinstance(cond_obj, dict):
            new_obj = {}
            for k, v in cond_obj.items():
                if k in ["$and", "$or"]:
                    new_obj[k] = [replace_field_refs(c) for c in v]
                elif k in ["$gt", "$lt", "$gte", "$lte", "$eq"]:
                    if isinstance(v, list):
                        new_obj[k] = []
                        for e in v:
                            if isinstance(e, str) and e.startswith("$"):
                                new_obj[k].append(f"$$item.{e[1:]}")
                            else:
                                new_obj[k].append(e)
                    else:
                        new_obj[k] = v
                elif isinstance(v, dict):
                    new_obj[k] = replace_field_refs(v)
                elif isinstance(v, str) and v in fields:
                    new_obj[k] = f"$$item.{v}"
                else:
                    new_obj[k] = v
            return new_obj
        return cond_obj

    filter_cond = replace_field_refs(condition)

    return {
        "$filter": {
            "input": f"${array_field}",
            "as": "item",
            "cond": filter_cond
        }
    }

def parse_aggregation_query(text, schema=None):
    doc = nlp(text.lower())
    collection = None
    pipeline = []

    for token in doc:
        if token.text in schema:
            collection = token.text
            break
    if not collection:
        return {"error": "Collection not found."}
    fields = schema[collection]["fields"]

    match_cond = {}
    if "where" in text:
        where_idx = text.index("where") + len("where")
        cond_text = text[where_idx:]
        stop_words = ["group", "unwind", "only show", "if"]
        for w in stop_words:
            if w in cond_text:
                cond_text = cond_text.split(w)[0].strip()
        match_cond = parse_natural_conditions(cond_text, fields)
        if match_cond:
            pipeline.append({"$match": match_cond})

    m_unwind = re.findall(r"unwind (\w+)", text)
    for array_field in m_unwind:
        if array_field in fields and fields[array_field] == "array":
            pipeline.append({"$unwind": f"${array_field}"})

    group_fields = []
    m_group = re.search(r"group(?:ed)? by ([\w ,]+)", text)
    if m_group:
        group_text = m_group.group(1)
        group_fields = [f.strip() for f in re.split(r",|and", group_text) if f.strip() in fields]

    agg_field = None
    for f, t in fields.items():
        if f in text and t in ("int", "float"):
            agg_field = f
            break

    if agg_field or "count" in text:
        group_stage = {}
        group_stage["_id"] = {f: f"${f}" for f in group_fields} if group_fields else None
        if "sum" in text or "total" in text:
            group_stage[f"total_{agg_field}"] = {"$sum": f"${agg_field}"}
        elif "average" in text or "avg" in text:
            group_stage[f"avg_{agg_field}"] = {"$avg": f"${agg_field}"}
        elif "count" in text:
            group_stage["count"] = {"$sum": 1}
        pipeline.append({"$group": group_stage})

    if "only show" in text:
        m_proj = re.search(r"only show (.+?)(?: if| unwind| group|$)", text)
        if m_proj:
            raw_fields = m_proj.group(1)
            proj_fields = [f.strip() for f in re.split(r",| and ", raw_fields) if f.strip() in fields]
            if proj_fields:
                pipeline.append({"$project": {f: 1 for f in proj_fields}})

    m_switch = re.search(r"if (.+?) then '(.+?)' else '(.+?)'", text)
    if m_switch and agg_field:
        switch_expr = parse_switch_expression(m_switch.group(0), agg_field)
        if switch_expr:
            pipeline.append({"$project": {f"{agg_field}_category": switch_expr}})

    m_filter = re.search(r"filter (\w+) where (.+)", text)
    if m_filter:
        array_field = m_filter.group(1)
        cond_text = "where " + m_filter.group(2)
        if array_field in fields and fields[array_field] == "array":
            filter_expr = parse_filter_expression(array_field, cond_text, fields)
            if filter_expr:
                pipeline.append({"$project": {f"filtered_{array_field}": filter_expr}})

    return {
        "collection": collection,
        "pipeline": pipeline
    }
