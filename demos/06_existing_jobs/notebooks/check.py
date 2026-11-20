# Databricks notebook source
# ruff: noqa: F821  (spark and dbutils are notebook globals)
# Fails the run for dates before the orders table starts, so the demo can show a failure.
run_date = dbutils.widgets.get("run_date")
if run_date < "2024-01-01":
    raise ValueError(f"run_date {run_date} is before the orders table starts")
dbutils.notebook.exit("ok")
