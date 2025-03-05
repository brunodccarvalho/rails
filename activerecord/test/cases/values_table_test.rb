# frozen_string_literal: true

require "cases/helper"
require "models/book"

class ValuesTableTest < ActiveRecord::TestCase
  fixtures :books

  def setup
    Arel::Table.engine = nil # should not rely on the global Arel::Table.engine

    @original_db_warnings_action = :ignore
    @connection = ActiveRecord::Base.lease_connection

    skip unless @connection.supports_values_tables?
  end

  def teardown
    Arel::Table.engine = ActiveRecord::Base
  end

  def test_arel_values_table_self_default_column_names
    table = Arel::Nodes::ValuesTable.new(:data, [[1, "one"], [2, "two"]])
    result = @connection.raw_exec_query(table.to_sql(ActiveRecord::Base))

    assert_equal ["column1", "column2"], result.columns
    assert_equal [[1, "one"], [2, "two"]], result.rows
  end

  def test_arel_values_table_self_supports_aliases
    table = Arel::Nodes::ValuesTable.new(:data, [[1, "one"], [2, "two"]], columns: %w[alias1 alias2])
    result = @connection.raw_exec_query(table.to_sql(ActiveRecord::Base))

    assert_equal ["alias1", "alias2"], result.columns
    assert_equal [[1, "one"], [2, "two"]], result.rows
  end

  def test_arel_values_table_derived
    table = Arel::Nodes::ValuesTable.new(:data, [[1, "one"], [2, "two"]])
    query = table.from.project(Arel.star)
    result = @connection.raw_exec_query(query.to_sql(ActiveRecord::Base))

    assert_equal ["column1", "column2"], result.columns
    assert_equal [[1, "one"], [2, "two"]], result.rows
  end

  def test_values_table_cte_then_join
    table = Arel::Nodes::ValuesTable.new(:data, [[1, "one"], [2, "two"]])
    query = Arel::SelectManager.new.with(table).from("data").project(Arel.star)
    result = @connection.raw_exec_query(query.to_sql(ActiveRecord::Base))

    assert_equal ["column1", "column2"], result.columns
    assert_equal [[1, "one"], [2, "two"]], result.rows
  end

  def test_values_table_cte_then_join_with_aliases
    table = Arel::Nodes::ValuesTable.new(:data, [[1, "one"], [2, "two"]], columns: %w[alias1 alias2])
    query = Arel::SelectManager.new.with(table).from("data").project(Arel.star)
    result = @connection.raw_exec_query(query.to_sql(ActiveRecord::Base))

    assert_equal ["alias1", "alias2"], result.columns
    assert_equal [[1, "one"], [2, "two"]], result.rows
  end
end
