from asynch.proto.columns.intcolumn import IntColumn


class BoolColumn(IntColumn):
    ch_type = "Bool"
    format = "b"
    int_size = 1
