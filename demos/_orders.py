"""The table and the question every data chapter shares.

Holding both constant is what lets each chapter change only the access path: the same
statement runs in a Connect session, in DuckDB over vended files, and on a SQL
warehouse.

No pyspark import, so every step can import this module whatever its environment. Each
demo directory links to this file the same way it links to `_env.py`.
"""

ORDERS = "main.retail.orders"

#: ORDERS without customer_id, the usual way to share a table with people who should
#: not see who bought what. A view has no storage of its own, so credential vending
#: cannot read it; every engine can.
ORDERS_VIEW = "main.retail.orders_public"

#: Where chapters 3 and 4 write the daily summary.
ORDERS_DAILY = "main.retail.orders_daily"

DAILY_SQL = """
SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
FROM {source}
GROUP BY order_date
ORDER BY order_date
"""


def source(table_name, version=None):
    """`table_name` as a FROM clause, at `version` when the reference is pinned.

    Identifiers and VERSION AS OF take no parameter markers, so the version is
    formatted in, as an int so nothing else can be.
    """
    if version is None:
        return table_name
    return "%s VERSION AS OF %d" % (table_name, int(version))


def normalize(frame):
    """Give results from different engines one dtype per column, for comparison.

    Engines disagree on date and decimal types, and floating-point sums differ in the
    last digits depending on the order rows were added in.
    """
    return (
        frame.assign(
            order_date=frame["order_date"].astype(str),
            orders=frame["orders"].astype("int64"),
            revenue=frame["revenue"].astype(float).round(2),
        )
        .sort_values("order_date")
        .reset_index(drop=True)
    )


def same_answer(left, right):
    left, right = normalize(left), normalize(right)
    return (
        len(left) == len(right)
        and (left["order_date"] == right["order_date"]).all()
        and (left["orders"] == right["orders"]).all()
        and ((left["revenue"] - right["revenue"]).abs() < 0.01).all()
    )
