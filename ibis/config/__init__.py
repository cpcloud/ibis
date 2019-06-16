from . import config as cf
from .config import option_context, options  # noqa: F401

cf.register_option('interactive', False, validator=cf.is_bool)
cf.register_option('verbose', False, validator=cf.is_bool)
cf.register_option('verbose_log', None)

cf.register_option(
    'graphviz_repr',
    True,
    """\
Whether to render expressions as GraphViz PNGs when repr-ing in a Jupyter
notebook.
""",
    validator=cf.is_bool,
)

cf.register_option('default_backend', None)

sql_default_limit_doc = """
Number of rows to be retrieved for an unlimited table expression
"""


with cf.config_prefix('sql'):
    cf.register_option('default_limit', 10000, sql_default_limit_doc)


impala_temp_db_doc = """
Database to use for temporary tables, views. functions, etc.
"""

impala_temp_hdfs_path_doc = """
HDFS path for storage of temporary data
"""


with cf.config_prefix('impala'):
    cf.register_option('temp_db', '__ibis_tmp', impala_temp_db_doc)
    cf.register_option(
        'temp_hdfs_path', '/tmp/ibis', impala_temp_hdfs_path_doc
    )


clickhouse_temp_db_doc = """
Database to use for temporary tables, views. functions, etc.
"""

with cf.config_prefix('clickhouse'):
    cf.register_option('temp_db', '__ibis_tmp', clickhouse_temp_db_doc)


with cf.config_prefix('bigquery'):
    cf.register_option('partition_col', 'PARTITIONTIME')
