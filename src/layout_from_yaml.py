import tables as tb
import re
import yaml
from functools import partial
from itertools import count

FILTERS = tb.Filters(9, 'blosc')

type_dict = {
    's' : tb.StringCol,
    'f' : tb.Float64Col,
    'i' : tb.Int64Col,
    'b' : tb.BoolCol
}


def mangle_dupes(columns):
    seen = set()
    new_cols = []
    for col in columns:
        i = 1
        name, type_ = next(iter(col.items()))
        new_name = name
        while new_name in seen:
            new_name = name + '_%d' % i
            i += 1
        seen.add(new_name)
        new_cols.append({new_name: type_})
    return new_cols


def reformat_names(columns):
    out = []
    for col in columns:
        ((colname, type_),) = col.items()
        reformatted = re.sub(r'\W+', '_', colname).strip('_')
        reformatted = '_' + reformatted if reformatted[0].isdigit() else reformatted
        out.append({reformatted: type_})
    return out


def preprocess_columns(layout):
    columns = layout['columns']
    if layout.get('reformat_names', True):
        columns = reformat_names(columns)
    return mangle_dupes(columns)


def get_fw_col_type(specs, pos):
    dflt = None

    if isinstance(specs, list):
        col_type_slice, dflt = specs
    else:
        col_type_slice = specs

    type_key, slc_range = col_type_slice[0], col_type_slice[1:].split('-')
    col_type = type_dict[type_key]

    slc = slice(*(int(s) for s in slc_range))

    if col_type is tb.StringCol:
        length = slc.stop - slc.start
        col_type = partial(col_type, length)

    if dflt is not None:
        return slc, col_type(pos=pos, dflt=dflt)

    return slc, col_type(pos=pos)


def get_csv_col_types(specs, table_pos, file_pos=None):
    dflt = None

    if file_pos is None:
        file_pos = table_pos

    if isinstance(specs, list):
        col_type_abrv, dflt = specs
    else:
        col_type_abrv = specs

    type_key = col_type_abrv[0]
    col_type = type_dict[type_key]

    if col_type is tb.StringCol:
        length = int(col_type_abrv[1:])
        col_type = partial(col_type, length)

    if dflt is not None:
        return file_pos, col_type(pos=table_pos, dflt=dflt)

    return file_pos, col_type(pos=table_pos)


def get_description_from_yaml(yaml_fname, encoding='utf-8'):
    layout = yaml.load(open(yaml_fname, encoding=encoding))
    is_fw = layout.get('delimiter') is None
    my_loc_dict = {}
    columns = preprocess_columns(layout)
    table_pos_iter = count()
    for i, col in enumerate(columns):
        ((col_name, specs),) = col.items()
        if specs is not None:
            table_pos = next(table_pos_iter)
            if is_fw:
                field_descr = get_fw_col_type(specs, table_pos)
            else:
                field_descr = get_csv_col_types(specs, table_pos, i)
            my_loc_dict[col_name] = field_descr
    return my_loc_dict


def get_file_and_table_description(loc_dict):
    file_descr = {field: val[0] for field, val in loc_dict.items()}
    table_descr = {field: val[1] for field, val in loc_dict.items()}
    return file_descr, table_descr


def write_fields(table, rows):
    person = table.row
    for i, row in enumerate(rows, start=1):

        for k, v in row.items():
            v = v.strip()
            if v:
                if type(person[k]) is bool:
                    person[k] = int(v)
                else:
                    person[k] = v
        person.append()
    table.flush()
    print('%d rows written' % i)