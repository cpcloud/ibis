import ibis


alltypes = ibis.table(
    name="alltypes",
    schema={
        "a": "int8",
        "b": "int16",
        "c": "int32",
        "d": "int64",
        "e": "float32",
        "f": "float64",
        "g": "string",
        "h": "boolean",
        "i": "timestamp",
        "j": "date",
        "k": "time",
    },
)
agg = alltypes.group_by([alltypes.a, alltypes.g]).aggregate(
    alltypes.f.sum().name("metric")
)
selfreference = agg.view()
joinprojection = agg.join_projection(
    selections=("agg",),
    rest=("selfreference",),
    hows=("inner",),
    predicates=("agg.g == selfreference.g",),
)
union = joinprojection.union(joinprojection.view())

result = union.select([union.a, union.g, union.metric])
