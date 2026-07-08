# Databricks notebook source
# MAGIC %pip install jinja2

# COMMAND ----------

parameters = [
    {
        "table": "musicmeta_dev.silver.factstream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration",
        "where": ""
    },
    {
        "table": "musicmeta_dev.silver.dimuser",
        "alias": "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name",
        "on": "factstream.user_id=dimuser.user_id",
        "where": ""
    },
    {
        "table": "musicmeta_dev.silver.dimtrack",
        "alias": "dimtrack",
        "cols": "dimtrack.track_id, dimtrack.track_name",
        "on": "factstream.track_id=dimtrack.track_id",
        "where": ""
    }
]

# COMMAND ----------

from jinja2 import Template

query_text = """
    SELECT
    {% for param in parameters %}
    {{ param['cols'] }}
    {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM
    {% for param in parameters %}
    {% if loop.first %}
    {{ param['table'] }} AS {{ param['alias'] }}
    {% endif %}
    {% endfor %}
    {% for param in parameters %}
    {% if not loop.first %}
    LEFT JOIN {{ param['table'] }} AS {{ param['alias'] }}
    ON {{ param['on'] }}
    {% endif %}
    {% endfor %}
    WHERE 1=1
    {% for param in parameters %}
    {% if param['where'] and param['where'] != "" %}
    AND {{ param['where'] }}
    {% endif %}
    {% endfor %}
"""

jinja_sql_str = Template(query_text)
query = jinja_sql_str.render(parameters=parameters)
print(query)
display(spark.sql(query))

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG musicmeta_dev 
# MAGIC SET MANAGED LOCATION 'abfss://databricksmetastore@musicmetadataadls.dfs.core.windows.net/musicmeta_dev';
# MAGIC DROP SCHEMA IF EXISTS musicmeta_dev.gold CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS musicmeta_dev.gold;