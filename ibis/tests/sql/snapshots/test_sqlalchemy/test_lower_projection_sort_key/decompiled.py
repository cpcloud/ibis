import ibis


star2 = ibis.table(
    name="star2", schema={"foo_id": "string", "value1": "float64", "value3": "float64"}
)
star1 = ibis.table(
    name="star1",
    schema={"c": "int32", "f": "float64", "foo_id": "string", "bar_id": "string"},
)
agg = star1.group_by(star1.foo_id).aggregate(star1.f.sum().name("total"))
joinprojection = agg.join_projection(
    selections=("agg", "star2.value1"),
    rest=("star2",),
    hows=("inner",),
    predicates=("agg.foo_id == star2.foo_id",),
)
proj = joinprojection.filter(joinprojection.total > 100)

result = proj.order_by(proj.total.desc())
