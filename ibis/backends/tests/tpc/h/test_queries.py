from __future__ import annotations

import pytest

import ibis
import ibis.selectors as s
from ibis import _
from ibis.backends.tests.errors import ClickHouseDatabaseError
from ibis.backends.tests.tpc.conftest import add_date, tpc_test


@tpc_test("h")
def test_01(lineitem):
    """Pricing Summary Report Query (Q1).

    The Pricing Summary Report Query provides a summary pricing report for all
    lineitems shipped as of a given date.  The  date is  within  60  - 120 days
    of  the  greatest  ship  date  contained  in  the database.  The query
    lists totals  for extended  price,  discounted  extended price, discounted
    extended price  plus  tax,  average  quantity, average extended price,  and
    average discount.  These  aggregates  are grouped  by RETURNFLAG  and
    LINESTATUS, and  listed  in ascending  order of RETURNFLAG and  LINESTATUS.
    A  count  of the  number  of  lineitems in each  group  is included.
    """
    discount_price = _.l_extendedprice * (1 - _.l_discount)
    charge = discount_price * (1 + _.l_tax)
    return (
        lineitem.filter(_.l_shipdate <= add_date("1998-12-01", dd=-90))
        .group_by(_.l_returnflag, _.l_linestatus)
        .agg(
            sum_qty=_.l_quantity.sum(),
            sum_base_price=_.l_extendedprice.sum(),
            sum_disc_price=discount_price.sum(),
            sum_charge=charge.sum(),
            avg_qty=_.l_quantity.mean(),
            avg_price=_.l_extendedprice.mean(),
            avg_disc=_.l_discount.mean(),
            count_order=_.count(),
        )
        .order_by(_.l_returnflag, _.l_linestatus)
    )


@pytest.mark.notyet(
    ["clickhouse"],
    raises=ClickHouseDatabaseError,
    reason="correlated subqueries don't exist in clickhouse",
)
@tpc_test("h")
def test_02(part, supplier, partsupp, nation, region):
    """Minimum Cost Supplier Query (Q2)"""

    REGION = "EUROPE"
    SIZE = 15
    TYPE = "BRASS"

    expr = (
        part.join(partsupp, part.p_partkey == partsupp.ps_partkey)
        .join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
        .filter(_.r_name == REGION, expr.p_partkey == _.ps_partkey)
    )

    return (
        expr.filter(
            _.p_size == SIZE,
            _.p_type.like(f"%{TYPE}"),
            _.r_name == REGION,
            _.ps_supplycost == subexpr.ps_supplycost.min(),
        )
        .select(
            _.s_acctbal,
            _.s_name,
            _.n_name,
            _.p_partkey,
            _.p_mfgr,
            _.s_address,
            _.s_phone,
            _.s_comment,
        )
        .order_by(_.s_acctbal.desc(), _.n_name, _.s_name, _.p_partkey)
        .limit(100)
    )


@tpc_test("h")
def test_03(customer, orders, lineitem):
    """Shipping Priority Query (Q3)"""
    MKTSEGMENT = "BUILDING"
    DATE = ibis.date("1995-03-15")

    return (
        customer.join(orders, customer.c_custkey == orders.o_custkey)
        .join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
        .filter(_.c_mktsegment == MKTSEGMENT, _.o_orderdate < DATE, _.l_shipdate > DATE)
        .group_by(_.l_orderkey, _.o_orderdate, _.o_shippriority)
        .agg(revenue=(_.l_extendedprice * (1 - _.l_discount)).sum())
        .relocate("revenue", after="l_orderkey")
        .order_by(_.revenue.desc(), _.o_orderdate)
        .limit(10)
    )


@pytest.mark.notyet(
    ["clickhouse"],
    raises=ClickHouseDatabaseError,
    reason="correlated subqueries don't exist in clickhouse",
)
@tpc_test("h")
def test_04(orders, lineitem):
    """Order Priority Checking Query (Q4)"""
    DATE = "1993-07-01"
    return (
        orders.filter(
            (
                (lineitem.l_orderkey == orders.o_orderkey)
                & (lineitem.l_commitdate < lineitem.l_receiptdate)
            ).any(),
            _.o_orderdate >= ibis.date(DATE),
            _.o_orderdate < add_date(DATE, dm=3),
        )
        .group_by(_.o_orderpriority)
        .agg(order_count=_.count())
        .order_by(_.o_orderpriority)
    )


@tpc_test("h")
def test_05(customer, lineitem, orders, supplier, nation, region):
    """Local Supplier Volume Query (Q5)"""
    NAME = "ASIA"
    DATE = "1994-01-01"

    return (
        customer.join(orders, customer.c_custkey == orders.o_custkey)
        .join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
        .join(supplier, lineitem.l_suppkey == supplier.s_suppkey)
        .join(
            nation,
            (customer.c_nationkey == supplier.s_nationkey)
            & (supplier.s_nationkey == nation.n_nationkey),
        )
        .join(region, nation.n_regionkey == region.r_regionkey)
        .filter(
            _.r_name == NAME,
            _.o_orderdate >= ibis.date(DATE),
            _.o_orderdate < add_date(DATE, dy=1),
        )
        .group_by(_.n_name)
        .agg(revenue=(_.l_extendedprice * (1 - _.l_discount)).sum())
        .order_by(_.revenue.desc())
    )


@tpc_test("h")
def test_06(lineitem):
    "Forecasting Revenue Change Query (Q6)"
    DATE = "1994-01-01"
    DISCOUNT = 0.06
    QUANTITY = 24

    return lineitem.filter(
        _.l_shipdate >= ibis.date(DATE),
        _.l_shipdate < add_date(DATE, dy=1),
        _.l_discount.between(round(DISCOUNT - 0.01, 2), round(DISCOUNT + 0.01, 2)),
        _.l_quantity < QUANTITY,
    ).agg(revenue=(_.l_extendedprice * _.l_discount).sum())


@tpc_test("h")
def test_07(supplier, lineitem, orders, customer, nation):
    "Volume Shipping Query (Q7)"
    NATION1 = "FRANCE"
    NATION2 = "GERMANY"
    DATE = "1995-01-01"

    n1 = nation
    n2 = nation.view()

    return (
        supplier.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
        .join(orders, orders.o_orderkey == lineitem.l_orderkey)
        .join(customer, customer.c_custkey == orders.o_custkey)
        .join(n1, supplier.s_nationkey == n1.n_nationkey)
        .join(n2, customer.c_nationkey == n2.n_nationkey)
        .select(
            n1.n_name.name("supp_nation"),
            n2.n_name.name("cust_nation"),
            _.l_shipdate,
            _.l_extendedprice,
            _.l_discount,
            _.l_shipdate.year().name("l_year"),
            (_.l_extendedprice * (1 - _.l_discount)).name("volume"),
        )
        .filter(
            ((_.cust_nation == NATION1) & (_.supp_nation == NATION2))
            | ((_.cust_nation == NATION2) & (_.supp_nation == NATION1)),
            _.l_shipdate.between(ibis.date(DATE), add_date(DATE, dy=2, dd=-1)),
        )
        .group_by(_.supp_nation, _.cust_nation, _.l_year)
        .agg(revenue=_.volume.sum())
        .order_by(s.all() & ~s.cols("revenue"))
    )


@tpc_test("h")
def test_08(part, supplier, region, lineitem, orders, customer, nation):
    """National Market Share Query (Q8)"""
    NATION = "BRAZIL"
    REGION = "AMERICA"
    TYPE = "ECONOMY ANODIZED STEEL"
    DATE = "1995-01-01"

    n1 = nation
    n2 = n1.view()

    return (
        part.join(lineitem, part.p_partkey == lineitem.l_partkey)
        .join(supplier, supplier.s_suppkey == lineitem.l_suppkey)
        .join(orders, lineitem.l_orderkey == orders.o_orderkey)
        .join(customer, orders.o_custkey == customer.c_custkey)
        .join(n1, customer.c_nationkey == n1.n_nationkey)
        .join(region, n1.n_regionkey == region.r_regionkey)
        .join(n2, supplier.s_nationkey == n2.n_nationkey)
        .select(
            _.o_orderdate.year().name("o_year"),
            (_.l_extendedprice * (1 - _.l_discount)).name("volume"),
            n2.n_name.name("nation"),
            _.r_name,
            _.o_orderdate,
            _.p_type,
        )
        .filter(
            _.r_name == REGION,
            _.o_orderdate.between(ibis.date(DATE), add_date(DATE, dy=2, dd=-1)),
            _.p_type == TYPE,
        )
        .mutate(nation_volume=ibis.cases((_.nation == NATION, _.volume), else_=0))
        .group_by(_.o_year)
        .agg(mkt_share=_.nation_volume.sum() / _.volume.sum())
        .order_by(_.o_year)
    )


@tpc_test("h")
def test_09(part, supplier, lineitem, partsupp, orders, nation):
    """Product Type Profit Measure Query (Q9)"""
    COLOR = "green"

    return (
        lineitem.join(supplier, supplier.s_suppkey == lineitem.l_suppkey)
        .join(
            partsupp,
            (partsupp.ps_suppkey == lineitem.l_suppkey)
            & (partsupp.ps_partkey == lineitem.l_partkey),
        )
        .join(part, part.p_partkey == lineitem.l_partkey)
        .join(orders, orders.o_orderkey == lineitem.l_orderkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .select(
            amount=(
                _.l_extendedprice * (1 - _.l_discount) - _.ps_supplycost * _.l_quantity
            ),
            o_year=_.o_orderdate.year(),
            nation=_.n_name,
            p_name=_.p_name,
        )
        .filter(_.p_name.like(f"%{COLOR}%"))
        .group_by(_.nation, _.o_year)
        .agg(sum_profit=_.amount.sum())
        .order_by(_.nation, _.o_year.desc())
    )


@tpc_test("h")
def test_10(customer, orders, lineitem, nation):
    """Returned Item Reporting Query (Q10)"""
    DATE = "1993-10-01"

    return (
        customer.join(orders, customer.c_custkey == orders.o_custkey)
        .join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
        .join(nation, customer.c_nationkey == nation.n_nationkey)
        .filter(
            (_.o_orderdate >= ibis.date(DATE)) & (_.o_orderdate < add_date(DATE, dm=3)),
            _.l_returnflag == "R",
        )
        .group_by(
            _.c_custkey,
            _.c_name,
            _.c_acctbal,
            _.n_name,
            _.c_address,
            _.c_phone,
            _.c_comment,
        )
        .agg(revenue=(_.l_extendedprice * (1 - _.l_discount)).sum())
        .relocate("revenue", after="c_name")
        .order_by(_.revenue.desc())
        .limit(20)
    )


@tpc_test("h")
def test_11(partsupp, supplier, nation):
    NATION = "GERMANY"
    FRACTION = 0.0001

    innerq = (
        partsupp.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
        .join(nation, nation.n_nationkey == supplier.s_nationkey)
        .filter(_.n_name == NATION)
        .agg(total=(_.ps_supplycost * _.ps_availqty).sum())
    )

    return (
        partsupp.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
        .join(nation, nation.n_nationkey == supplier.s_nationkey)
        .filter(_.n_name == NATION)
        .group_by(_.ps_partkey)
        .agg(value=(_.ps_supplycost * _.ps_availqty).sum())
        .filter(_.value > innerq.total * FRACTION)
        .order_by(_.value.desc())
    )


@tpc_test("h")
def test_12(orders, lineitem):
    """'Shipping Modes and Order Priority Query (Q12)

    This query determines whether selecting less expensive modes of shipping is
    negatively affecting the critical-prior- ity orders by causing more parts
    to be received by customers after the committed date."""
    SHIPMODE1 = "MAIL"
    SHIPMODE2 = "SHIP"
    DATE = "1994-01-01"

    return (
        orders.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
        .filter(
            _.l_shipmode.isin([SHIPMODE1, SHIPMODE2]),
            _.l_commitdate < _.l_receiptdate,
            _.l_shipdate < _.l_commitdate,
            _.l_receiptdate >= ibis.date(DATE),
            _.l_receiptdate < add_date(DATE, dy=1),
        )
        .group_by(_.l_shipmode)
        .agg(
            high_line_count=_.o_orderpriority.cases(
                ("1-URGENT", 1),
                ("2-HIGH", 1),
                else_=0,
            ).sum(),
            low_line_count=_.o_orderpriority.cases(
                ("1-URGENT", 0),
                ("2-HIGH", 0),
                else_=1,
            ).sum(),
        )
        .order_by(_.l_shipmode)
    )


@tpc_test("h")
def test_13(customer, orders):
    """Customer Distribution Query (Q13)

    This query seeks relationships between customers and the size of their
    orders."""

    WORD1 = "special"
    WORD2 = "requests"

    return (
        customer.left_join(
            orders,
            (customer.c_custkey == orders.o_custkey)
            & ~orders.o_comment.like(f"%{WORD1}%{WORD2}%"),
        )
        .group_by(_.c_custkey)
        .agg(c_count=_.o_orderkey.count())
        .group_by(_.c_count)
        .agg(custdist=_.count())
        .order_by(_.custdist.desc(), _.c_count.desc())
    )


@tpc_test("h")
def test_14(part, lineitem):
    """Promotion Effect Query (Q14)

    This query monitors the market response to a promotion such as TV
    advertisements or a special campaign."""

    DATE = "1995-09-01"

    return (
        lineitem.join(part, lineitem.l_partkey == part.p_partkey)
        .filter(_.l_shipdate >= ibis.date(DATE), _.l_shipdate < add_date(DATE, dm=1))
        .mutate(revenue=_.l_extendedprice * (1 - _.l_discount))
        .mutate(promo_revenue=_.p_type.like("PROMO%").ifelse(_.revenue, 0))
        .agg(promo_revenue=100 * _.promo_revenue.sum() / _.revenue.sum())
    )


@tpc_test("h")
@pytest.mark.notyet(
    ["trino"],
    reason="unreliable due to floating point differences in repeated evaluations of identical subqueries",
    raises=AssertionError,
    strict=False,
)
def test_15(lineitem, supplier):
    """Top Supplier Query (Q15)"""

    DATE = "1996-01-01"

    rev = (
        lineitem.filter(
            _.l_shipdate >= ibis.date(DATE),
            _.l_shipdate < add_date(DATE, dm=3),
        )
        .group_by(lineitem.l_suppkey)
        .agg(total_revenue=(_.l_extendedprice * (1 - _.l_discount)).sum())
    )

    return (
        supplier.join(rev, supplier.s_suppkey == rev.l_suppkey)
        .filter(_.total_revenue == rev.total_revenue.max())
        .select(_.s_suppkey, _.s_name, _.s_address, _.s_phone, _.total_revenue)
        .order_by(_.s_suppkey)
    )


@tpc_test("h")
def test_16(partsupp, part, supplier):
    """Parts/Supplier Relationship Query (Q16)

    This query finds out how many suppliers can supply parts with given
    attributes. It might be used, for example, to determine whether there is
    a sufficient number of suppliers for heavily ordered parts."""

    BRAND = "Brand#45"
    TYPE = "MEDIUM POLISHED"
    SIZES = (49, 14, 23, 45, 19, 3, 36, 9)

    return (
        partsupp.join(part, part.p_partkey == partsupp.ps_partkey)
        .filter(
            _.p_brand != BRAND,
            ~_.p_type.like(f"{TYPE}%"),
            _.p_size.isin(SIZES),
            ~_.ps_suppkey.isin(
                supplier.filter(
                    supplier.s_comment.like("%Customer%Complaints%")
                ).s_suppkey
            ),
        )
        .group_by(_.p_brand, _.p_type, _.p_size)
        .agg(supplier_cnt=_.ps_suppkey.nunique())
        .order_by(_.supplier_cnt.desc(), _.p_brand, _.p_type, _.p_size)
    )


@pytest.mark.notyet(
    ["clickhouse"],
    raises=ClickHouseDatabaseError,
    reason="correlated subqueries don't exist in clickhouse",
)
@tpc_test("h")
def test_17(lineitem, part):
    """Small-Quantity-Order Revenue Query (Q17)

    This query determines how much average yearly revenue would be lost if
    orders were no longer filled for small quantities of certain parts. This
    may reduce overhead expenses by concentrating sales on larger shipments."""
    BRAND = "Brand#23"
    CONTAINER = "MED BOX"

    q = lineitem.join(part, part.p_partkey == lineitem.l_partkey)

    return q.filter(
        _.p_brand == BRAND,
        _.p_container == CONTAINER,
        _.l_quantity
        < 0.2 * lineitem.filter(_.l_partkey == q.p_partkey).l_quantity.mean(),
    ).agg(avg_yearly=_.l_extendedprice.sum() / 7.0)


@tpc_test("h")
def test_18(customer, orders, lineitem):
    """Large Volume Customer Query (Q18)

    The Large Volume Customer Query ranks customers based on their having
    placed a large quantity order. Large quantity orders are defined as those
    orders whose total quantity is above a certain level."""

    QUANTITY = 300

    return (
        customer.join(orders, customer.c_custkey == orders.o_custkey)
        .join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
        .filter(
            _.o_orderkey.isin(
                lineitem.group_by(_.l_orderkey)
                .agg(qty_sum=_.l_quantity.sum())
                .filter(_.qty_sum > QUANTITY)
                .l_orderkey
            )
        )
        .group_by(_.c_name, _.c_custkey, _.o_orderkey, _.o_orderdate, _.o_totalprice)
        .agg(sum_qty=_.l_quantity.sum())
        .order_by(_.o_totalprice.desc(), _.o_orderdate)
        .limit(100)
    )


@tpc_test("h")
def test_19(lineitem, part):
    """Discounted Revenue Query (Q19)

    The Discounted Revenue Query reports the gross discounted revenue
    attributed to the sale of selected parts handled in a particular manner.
    This query is an example of code such as might be produced programmatically
    by a data mining tool."""

    QUANTITY1 = 1
    QUANTITY2 = 10
    QUANTITY3 = 20
    BRAND1 = "Brand#12"
    BRAND2 = "Brand#23"
    BRAND3 = "Brand#34"

    return (
        lineitem.join(part, part.p_partkey == lineitem.l_partkey)
        .filter(
            (
                (_.p_brand == BRAND1)
                & (_.p_container.isin(("SM CASE", "SM BOX", "SM PACK", "SM PKG")))
                & (_.l_quantity >= QUANTITY1)
                & (_.l_quantity <= QUANTITY1 + 10)
                & (_.p_size.between(1, 5))
                & (_.l_shipmode.isin(("AIR", "AIR REG")))
                & (_.l_shipinstruct == "DELIVER IN PERSON")
            )
            | (
                (_.p_brand == BRAND2)
                & (_.p_container.isin(("MED BAG", "MED BOX", "MED PKG", "MED PACK")))
                & (_.l_quantity >= QUANTITY2)
                & (_.l_quantity <= QUANTITY2 + 10)
                & (_.p_size.between(1, 10))
                & (_.l_shipmode.isin(("AIR", "AIR REG")))
                & (_.l_shipinstruct == "DELIVER IN PERSON")
            )
            | (
                (_.p_brand == BRAND3)
                & (_.p_container.isin(("LG CASE", "LG BOX", "LG PACK", "LG PKG")))
                & (_.l_quantity >= QUANTITY3)
                & (_.l_quantity <= QUANTITY3 + 10)
                & (_.p_size.between(1, 15))
                & (_.l_shipmode.isin(("AIR", "AIR REG")))
                & (_.l_shipinstruct == "DELIVER IN PERSON")
            )
        )
        .agg(revenue=(_.l_extendedprice * (1 - _.l_discount)).sum())
    )


@pytest.mark.notyet(
    ["clickhouse"],
    raises=ClickHouseDatabaseError,
    reason="correlated subqueries don't exist in clickhouse",
)
@tpc_test("h")
def test_20(supplier, nation, partsupp, part, lineitem):
    """Potential Part Promotion Query (Q20)

    The Potential Part Promotion Query identifies suppliers in a particular
    nation having selected parts that may be candidates for a promotional
    offer."""
    COLOR = "forest"
    DATE = "1994-01-01"
    NATION = "CANADA"

    return (
        supplier.join(nation, supplier.s_nationkey == nation.n_nationkey)
        .filter(
            _.n_name == NATION,
            _.s_suppkey.isin(
                partsupp.filter(
                    _.ps_partkey.isin(
                        part.filter(_.p_name.like(f"{COLOR}%")).p_partkey
                    ),
                    _.ps_availqty
                    > 0.5
                    * lineitem.filter(
                        _.l_partkey == partsupp.ps_partkey,
                        _.l_suppkey == partsupp.ps_suppkey,
                        _.l_shipdate >= ibis.date(DATE),
                        _.l_shipdate < add_date(DATE, dy=1),
                    ).l_quantity.sum(),
                ).ps_suppkey
            ),
        )
        .select(_.s_name, _.s_address)
        .order_by(_.s_name)
    )


@pytest.mark.notyet(
    ["clickhouse"],
    raises=ClickHouseDatabaseError,
    reason="correlated subqueries don't exist in clickhouse",
)
@tpc_test("h")
def test_21(supplier, lineitem, orders, nation):
    """Suppliers Who Kept Orders Waiting Query (Q21)

    This query identifies certain suppliers who were not able to ship required
    parts in a timely manner."""
    NATION = "SAUDI ARABIA"

    L2 = lineitem.view()
    L3 = lineitem.view()

    q = (
        supplier.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
        .join(orders, orders.o_orderkey == lineitem.l_orderkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .select(
            _.l_orderkey.name("l1_orderkey"),
            _.o_orderstatus,
            _.l_receiptdate,
            _.l_commitdate,
            _.l_suppkey.name("l1_suppkey"),
            _.s_name,
            _.n_name,
        )
    )

    return (
        q.filter(
            q.o_orderstatus == "F",
            q.l_receiptdate > q.l_commitdate,
            q.n_name == NATION,
            ((L2.l_orderkey == q.l1_orderkey) & (L2.l_suppkey != q.l1_suppkey)).any(),
            ~(
                (
                    (L3.l_orderkey == q.l1_orderkey)
                    & (L3.l_suppkey != q.l1_suppkey)
                    & (L3.l_receiptdate > L3.l_commitdate)
                ).any()
            ),
        )
        .group_by(_.s_name)
        .agg(numwait=_.count())
        .order_by(_.numwait.desc(), _.s_name)
        .limit(100)
    )


@pytest.mark.notyet(
    ["clickhouse"],
    raises=ClickHouseDatabaseError,
    reason="correlated subqueries don't exist in clickhouse",
)
@tpc_test("h")
def test_22(customer, orders):
    """Global Sales Opportunity Query (Q22)

    The Global Sales Opportunity Query identifies geographies where there are
    customers who may be likely to make a purchase."""

    COUNTRY_CODES = ("13", "31", "23", "29", "30", "18", "17")

    q = customer.filter(
        _.c_acctbal > 0.00, _.c_phone.substr(0, 2).isin(COUNTRY_CODES)
    ).agg(avg_bal=_.c_acctbal.mean())

    return (
        customer.filter(
            _.c_phone.substr(0, 2).isin(COUNTRY_CODES),
            _.c_acctbal > q.avg_bal,
            ~(orders.o_custkey == customer.c_custkey).any(),
        )
        .select(_.c_phone.substr(0, 2).name("cntrycode"), _.c_acctbal)
        .group_by(_.cntrycode)
        .agg(numcust=_.count(), totacctbal=_.c_acctbal.sum())
        .order_by(_.cntrycode)
    )


def test_all_queries_are_written():
    variables = globals()
    numbers = range(1, 23)
    query_numbers = set(numbers)

    # remove query numbers that are implemented
    for query_number in numbers:
        if f"test_{query_number:02d}" in variables:
            query_numbers.remove(query_number)

    remaining_queries = sorted(query_numbers)
    assert not remaining_queries
