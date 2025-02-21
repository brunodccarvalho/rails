# frozen_string_literal: true

module Arel # :nodoc: all
  module Nodes
    class ValuesTable < Arel::Nodes::Node
      attr_reader :values, :name, :table_alias, :column_aliases

      # column_aliases is not supported in SQLite and will error out later if specified
      def initialize(name, rows, column_aliases = nil)
        name = name.name if name.is_a?(Symbol)
        @name = @table_alias = name
        @width = rows.first.size
        @values = ValuesList.new(rows)
        @column_aliases = column_aliases&.map(&:to_s)
      end

      def column_aliases_or_default_names
        @column_aliases_or_default_names ||= @column_aliases || (1..@width).map { |i| "column#{i}" }
      end

      def [](name, table = self)
        # Defer default column names to sqlite, the adapter that does not support aliases
        name = "column#{name + 1}" if name.is_a?(Integer) && @column_aliases.nil?
        name = name.name if name.is_a?(Symbol)
        Arel::Attribute.new(table, name)
      end

      def hash
        [self.class, @values, @name, @column_aliases].hash
      end

      def eql?(other)
        self.class == other.class &&
          @name == other.name &&
          @values == other.values &&
          @column_aliases == other.column_aliases
      end
      alias :== :eql?
    end
  end
end
