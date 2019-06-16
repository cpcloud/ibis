import csv
import tempfile
from posixpath import join as pjoin

from .. import util
from ..config import options
from ..expr import schema as sch


class DataFrameWriter:

    """
    Interface class for writing pandas objects to Impala tables

    Class takes ownership of any temporary data written to HDFS
    """

    def __init__(self, client, df, path=None):
        self.client = client
        self.hdfs = client.hdfs
        self.df = df
        self.temp_hdfs_dirs = []

    def write_temp_csv(self):
        temp_hdfs_dir = pjoin(
            options.impala.temp_hdfs_path, 'pandas_{}'.format(util.guid())
        )
        self.hdfs.mkdir(temp_hdfs_dir)

        # Keep track of the temporary HDFS file
        self.temp_hdfs_dirs.append(temp_hdfs_dir)

        # Write the file to HDFS
        hdfs_path = pjoin(temp_hdfs_dir, '0.csv')

        self.write_csv(hdfs_path)

        return temp_hdfs_dir

    def write_csv(self, path):
        with tempfile.NamedTemporaryFile() as f:
            # Write the DataFrame to the temporary file path
            if options.verbose:
                util.log(
                    'Writing DataFrame to temporary file {}'.format(f.name)
                )

            self.df.to_csv(
                f.name,
                header=False,
                index=False,
                sep=',',
                quoting=csv.QUOTE_NONE,
                escapechar='\\',
                na_rep='#NULL',
            )
            f.seek(0)

            if options.verbose:
                util.log('Writing CSV to: {0}'.format(path))

            self.hdfs.put(path, f.name)
        return path

    def get_schema(self):
        # define a temporary table using delimited data
        return sch.infer(self.df)

    def delimited_table(self, csv_dir, name=None, database=None):
        temp_delimited_name = 'ibis_tmp_pandas_{0}'.format(util.guid())
        schema = self.get_schema()

        return self.client.delimited_file(
            csv_dir,
            schema,
            name=temp_delimited_name,
            database=database,
            delimiter=',',
            na_rep='#NULL',
            escapechar='\\\\',
            external=True,
            persist=False,
        )

    def __del__(self):
        try:
            self.cleanup()
        except Exception:
            pass

    def cleanup(self):
        for path in self.temp_hdfs_dirs:
            self.hdfs.rmdir(path)
        self.temp_hdfs_dirs = []
        self.csv_dir = None


def write_temp_dataframe(client, df):
    writer = DataFrameWriter(client, df)
    path = writer.write_temp_csv()
    return writer.delimited_table(path)
