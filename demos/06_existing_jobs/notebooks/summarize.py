# Databricks notebook source
# ruff: noqa: F821  (spark and dbutils are notebook globals)
# Writes the daily summary up to run_date and reports what it wrote.
import json

run_date = dbutils.widgets.get("run_date")
target = dbutils.widgets.get("target")
daily = spark.sql(
    f"""
    SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
    FROM main.retail.orders
    WHERE order_date <= DATE'{run_date}'
    GROUP BY order_date
    """
)
daily.write.mode("overwrite").saveAsTable(target)
dbutils.notebook.exit(json.dumps({"table": target, "days": daily.count()}))
