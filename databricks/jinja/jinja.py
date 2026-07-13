# Databricks notebook source

dbutils.widgets.text("catalog_name", "musicmeta_dev", "Target Catalog")
catalog = dbutils.widgets.get("catalog_name")

# COMMAND ----------
%pip install jinja2==3.1.3

parameters = [
    {
        "table": f"{catalog}.silver.factstream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration",
        "where": ""
    },
    {
        "table": f"{catalog}.silver.dimuser",
        "alias": "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name",
        "on": "factstream.user_id=dimuser.user_id",
        "where": ""
    },
    {
        "table": f"{catalog}.silver.dimtrack",
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

