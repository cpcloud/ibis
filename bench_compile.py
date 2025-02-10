from __future__ import annotations

from datetime import date

import ibis

ibis.options.interactive = True
con = ibis.connect("duckdb://")

table_1 = con.read_parquet("./github_issue/table_1.parquet.zst", table_name="proc")
table_2 = con.read_parquet("./github_issue/table_2.parquet.zst")

add_col_value = (
    table_1
    # Change this SQL code to ibis syntax when features are released
    .sql(
        """
        SELECT
          *,
          CAST(
            LIST_MODE(
              LIST_REVERSE_SORT(
                LIST_FILTER(
                  LIST_TRANSFORM(
                    c_id,
                    (x, i) -> CASE
                                WHEN x IS NOT NULL THEN
                                  ROUND((e[i] - d[i]) * 100 / e[i], 0)
                              ELSE NULL
                              END
                  ),
                  x -> x BETWEEN 0 AND 100
                )
              )
            ) AS INTEGER
          ) AS col_value
        FROM proc
        """
    )
)

add_col_value2 = add_col_value.mutate(
    col_value_2=ibis.coalesce(
        (add_col_value.d == None)
        & (add_col_value.e == None)
        & (add_col_value.f == None)
        & (add_col_value.c_id == None)
        & (add_col_value.c_text == None),
        False,
    )
)

add_col_1_counter_preprocessed = (
    table_1.select("a_id", "b_id", table_1.c_id.unnest())
    .value_counts()
    .order_by(ibis.desc("a_id_b_id_c_id_count"))
)

add_col_1_counter = add_col_1_counter_preprocessed.group_by("a_id", "b_id").agg(
    col_1_counter=ibis.struct(
        [
            ("key", add_col_1_counter_preprocessed.c_id),
            (
                "counts",
                add_col_1_counter_preprocessed.a_id_b_id_c_id_count.cast("uint32"),
            ),
        ]
    ).collect(where=add_col_1_counter_preprocessed.c_id != None)
)

add_col_2_counter_preprocessed = (
    table_1.select("a_id", "b_id", table_1.c_text.unnest())
    .value_counts()
    .order_by(ibis.desc("a_id_b_id_c_text_count"))
)
add_col_2_counter = add_col_2_counter_preprocessed.group_by("a_id", "b_id").agg(
    col_2_counter=ibis.struct(
        [
            ("key", add_col_2_counter_preprocessed.c_text),
            (
                "counts",
                add_col_2_counter_preprocessed.a_id_b_id_c_text_count.cast("uint32"),
            ),
        ]
    ).collect(where=add_col_2_counter_preprocessed.c_text != "")
)

join_table_2 = (
    add_col_value2.join(table_2, "a_id")
    .join(
        add_col_1_counter,
        [
            add_col_value2.a_id == add_col_1_counter.a_id,
            add_col_value2.b_id == add_col_1_counter.b_id,
        ],
        how="left",
        rname="{name}_right",
    )
    .join(
        add_col_2_counter,
        [
            ibis.coalesce(add_col_value2.a_id, add_col_1_counter.a_id)
            == add_col_2_counter.a_id,
            ibis.coalesce(add_col_value2.b_id, add_col_1_counter.b_id)
            == add_col_2_counter.b_id,
        ],
        how="left",
        rname="{name}_right_2",
    )
    .mutate(
        ibis.coalesce(
            add_col_value2.a_id, add_col_1_counter.a_id, add_col_2_counter.a_id
        ).name("a_id"),
        ibis.coalesce(
            add_col_value2.b_id, add_col_1_counter.b_id, add_col_2_counter.b_id
        ).name("b_id"),
        *[
            ibis.cases(
                (add_col_value2.c_id[i] == None, None), else_=add_col_value2.d[i]
            ).name(f"col_val_x{i}")
            for i in range(7)
        ],
    )
)

add_col_3_preprocessed = join_table_2.mutate(
    *[join_table_2.c_id[i].name(f"c_id_{i}") for i in range(7)],
    col_3=join_table_2.c_id.filter(lambda x: x != None).length().cast("uint8"),
    col_val_x_without_0=ibis.array(
        [getattr(join_table_2, f"col_val_x{i}") for i in range(7)]
    ).filter(lambda x: x != 0),
)

add_col3 = add_col_3_preprocessed.mutate(
    col_x_avg=add_col_3_preprocessed.col_val_x_without_0.means()
    .round(2)
    .cast("float32"),
    col_x_min=add_col_3_preprocessed.col_val_x_without_0.mins(),
    col_x_max=add_col_3_preprocessed.col_val_x_without_0.maxs(),
    col_x_filetered=add_col_3_preprocessed.d.filter(lambda x: x > 0),
    f_filetered=add_col_3_preprocessed.e.filter(lambda x: x > 0),
    e_filetered=add_col_3_preprocessed.f.filter(lambda x: x > 0),
)

year, week = 2025, 2
last_day_of_week = date.fromisocalendar(year, week, 7)
add_col_4_preprocessed = add_col3.mutate(
    *[add_col3.d[i].name(f"ab_{i}") for i in range(7)],
    *[add_col3.e[i].name(f"e_{i}") for i in range(7)],
    *[add_col3.f[i].name(f"f_{i}") for i in range(7)],
    count_col_y=add_col3.e.filter(lambda x: x > 0).length().cast("uint8"),
    ab_avg=add_col3.d.filter(lambda x: x > 0).means().round(2).cast("float32"),
    ab_min=add_col3.d.filter(lambda x: x > 0).mins(),
    ab_max=add_col3.d.filter(lambda x: x > 0).maxs(),
    e_avg=add_col3.e.filter(lambda x: x > 0).means().round(2).cast("float32"),
    e_min=add_col3.e.filter(lambda x: x > 0).mins(),
    e_max=add_col3.e.filter(lambda x: x > 0).maxs(),
    f_avg=add_col3.f.filter(lambda x: x > 0).means().round(2).cast("float32"),
    f_min=add_col3.f.filter(lambda x: x > 0).mins(),
    f_max=add_col3.f.filter(lambda x: x > 0).maxs(),
)

add_col_4 = add_col_4_preprocessed.mutate(
    count_col_z=((7 - add_col_4_preprocessed.count_col_y) % 7).cast("uint8"),
    last_day_of_week=ibis.date(last_day_of_week),
)

add_col_5_preprocessed = add_col_4.mutate(
    col_5=ibis.cases(
        (
            (add_col_4.h == False) & (add_col_4.g.contains(False)),
            ibis.array(
                [
                    ibis.cases(
                        (i < add_col_4.g.index(False), False), else_=add_col_4.g[i]
                    )
                    for i in range(7)
                ]
            ),
        ),
        (
            (add_col_4.h == False) & (add_col_4.g.mins() == True),
            ibis.array([False] * 7),
        ),
        else_=add_col_4.g,
    )
)

add_col_5 = add_col_5_preprocessed.mutate(
    *[add_col_5_preprocessed.g[i].name(f"col_5_{i}") for i in range(7)],
    *[
        ibis.cases(
            (add_col_5_preprocessed.col_5[i] == None, False),
            else_=add_col_5_preprocessed.col_5[i],
        ).name(f"col_6_{i}")
        for i in range(7)
    ],
    count_col_6=add_col_5_preprocessed.col_5.map(
        lambda x: ibis.coalesce(x.cast("int"), 0)
    )
    .sums()
    .cast("uint8"),
).drop(
    "c_id",
    "c_text",
    "col_val_x_without_0",
    "d",
    "e",
    "f",
    "g",
    "col_5",
    "h",
    "last_day_of_week",
    "a_id_right",
    "b_id_right",
    "a_id_right_2",
    "b_id_right_2",
    "col_x_filetered",
    "f_filetered",
    "e_filetered",
)

comp = add_col_5.compile

import pyinstrument

with pyinstrument.profile():
    comp()
