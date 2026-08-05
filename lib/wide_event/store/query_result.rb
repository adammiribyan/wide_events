module WideEvent
  module Store
    # An immutable, already-bounded result of a JSON /v1/query request
    # (store/internal/httpapi/query.go's `{"columns":...,"rows":...,"truncated":...}`
    # body, parsed). `rows` is an array of arrays in column order; a `nil`
    # element is a SQL NULL. `truncated` is true when the store cut the
    # result off at its row or response-size limit.
    class QueryResult
      attr_reader :columns, :rows, :truncated

      def initialize(columns:, rows:, truncated:)
        @columns = columns.map(&:to_s).freeze
        @rows = rows.map { |row| row.freeze }.freeze
        @truncated = truncated == true
        freeze
      end
    end
  end
end
