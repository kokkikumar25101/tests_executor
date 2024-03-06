# test_ibis_duckdb.py
import pytest
import ibis
import duckdb

class TestIbisDuckDB:
    @classmethod
    def setup_class(cls):
        # Connect to DuckDB
        cls.conn = duckdb.connect()

    @classmethod
    def teardown_class(cls):
        # Close the connection
        cls.conn.close()

    def setup_method(self, method):
        # Create a test table
        self.conn.execute('CREATE TABLE my_table (id INT, name STRING)')

    def teardown_method(self, method):
        # Drop the test table
        self.conn.execute('DROP TABLE my_table')

    @pytest.mark.parametrize('data, expected', [
        ([(1, 'John'), (2, 'Jane')], ['John', 'Jane']),
        # Add more test cases here
    ])
    def test_query(self, data, expected):
        # Insert test data
        self.conn.execute('INSERT INTO my_table VALUES {}'.format(', '.join(map(str, data))))

        # Use IBIS to query the table
        ibis_conn = ibis.duckdb.connect(database=self.conn)
        table = ibis_conn.table('my_table')
        result = table.execute()

        # Perform assertions on the query result
        assert len(result) == len(data)
        assert result['name'].tolist() == expected