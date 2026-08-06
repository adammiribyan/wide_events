require "json"
require "csv"

module WideEvent
  module Store
    # Renders a bounded QueryResult for a terminal (table), for machine
    # consumption (json), or for a spreadsheet/pipe (csv). Every format
    # reads the whole (already-capped) result once; none of them stream or
    # re-request data, and none of them ever emit ANSI escape sequences —
    # the table formatter is plain text whether or not `io` is a TTY.
    module Formatter
      NULL_TEXT = "NULL"

      def self.table(result, io:)
        header = result.columns
        body_rows = result.rows.map { |row| row.map { |value| value.nil? ? NULL_TEXT : value.to_s } }
        widths = column_widths(header, body_rows)

        io.puts(row_line(header, widths))
        io.puts(separator_line(widths))
        body_rows.each { |row| io.puts(row_line(row, widths)) }
        io.puts("(truncated: the store capped this result)") if result.truncated
      end

      def self.json(result, io:)
        io.puts(JSON.generate(columns: result.columns, rows: result.rows, truncated: result.truncated))
      end

      def self.csv(result, io:)
        io.print(CSV.generate_line(result.columns))
        result.rows.each do |row|
          # Pass `nil` through as-is (CSV renders it as an empty, unquoted
          # field) rather than the string "" (which CSV quotes as `""` to
          # keep it distinct from a genuinely absent field) — matching the
          # store's own CSV null rendering (query/executor.go's csvValue).
          io.print(CSV.generate_line(row.map { |value| value.nil? ? nil : value.to_s }))
        end
      end

      def self.column_widths(header, body_rows)
        widths = header.map(&:length)
        body_rows.each do |row|
          row.each_with_index { |value, index| widths[index] = [ widths[index], value.length ].max }
        end
        widths
      end
      private_class_method :column_widths

      def self.row_line(values, widths)
        "| " + values.each_with_index.map { |value, index| value.ljust(widths[index]) }.join(" | ") + " |"
      end
      private_class_method :row_line

      def self.separator_line(widths)
        "+" + widths.map { |width| "-" * (width + 2) }.join("+") + "+"
      end
      private_class_method :separator_line
    end
  end
end
