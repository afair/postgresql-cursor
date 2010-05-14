# PostgreSQLCursor
# An extenstion for PostgreSQL adapter to select large result sets, buffered, and "fetched" 
# from the buffer. Hashes are returned for each row instead of an ActiveRecord subcless.
# Warning: This file must be laoded after ActiveRecord. Probably as a merb dependency gem/plugin.
class ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
  
  # Starts buffered result set processing for a given SQL statement. The DB
  def open_cursor(sql, name="csr", buffer_size=10_000)
    sql = replace_params(*(sql.flatten)) if sql.is_a?(Array)
    @cursors ||= {}
    @cursors[name] = {}
    @cursors[name][:sql] = sql
    #puts "OPEN_CURSOR #{sql}"
    @cursors[name][:result] = execute("declare #{name} cursor for #{sql}")
    @cursors[name][:state] = :empty
    @cursors[name][:buffer_size] = buffer_size
    @cursors[name][:buffer] = nil
  end
  
  # Fetches the next block of rows into memory
  def fetch_buffer(name='csr') #:nodoc:
    return unless @cursors[name][:state] == :empty
    @cursors[name][:result] = execute("fetch #{@cursors[name][:buffer_size]} from #{name}")
    @cursors[name][:buffer] = @cursors[name][:result].collect {|row| row } # result
    @cursors[name][:state]  = @cursors[name][:buffer].size > 0 ? :buffered : :eof
  end

  # Returns the next row from the cursor, or nil when end of data.
  # The row returned is a hash[:colname]
  def fetch_cursor(name='csr')
    raise ArgumentError, "Cursor #{name} not open" unless @cursors.has_key?(name)
    fetch_buffer(name) if @cursors[name][:state] == :empty
    return nil if @cursors[name][:state] == :eof
    @cursors[name][:state] = :empty if @cursors[name][:buffer].size <= 1
    row = @cursors[name][:buffer].shift
    row.is_a?(Hash) ? row.symbolize_keys : row
  end

  # Taken from PostgreSQLAdapter#select
  def row_tuple_to_hash(name, row) #:nodoc:
    hashed_row = {}
    @cursors[name][:field] ||= @cursors[name][:result].fields
    row.each_index do |cel_index|
      column = row[cel_index]

      case @cursors[name][:result].type(cel_index)
        when 17 #BYTEA_COLUMN_TYPE_OID
          column = unescape_bytea(column)
        when 1184, 1114 #TIMESTAMPTZOID, TIMESTAMPOID
          column = cast_to_time(column)
        when 1700 #NUMERIC_COLUMN_TYPE_OID
          column = column.to_d if column.respond_to?(:to_d)
      end

      hashed_row[@cursors[name][:field][cel_index].to_sym] = column
    end
    hashed_row
  end
  
  # Closes the cursor to clean up resources
  def close_cursor(name='csr')
    pg_result = execute("close #{name}")
    @cursors.delete(name)
  end
  
  # Iterates over a cursor within a transaction block
  def cursor_eachrow(sql, name='csr', transaction=true, buffer_size=10000)
    begin_db_transaction if transaction
    open_cursor(sql, name, buffer_size)
    count = 0 
    while (row = fetch_cursor(name)) do
      count+= 1
      #puts "EACH CSR #{row.inspect}"
      yield row
    end
    close_cursor(name)
    commit_db_transaction if transaction
    count
  end
  
  # Performs a simple parameter substitution of '?'s in the sql statement
  def replace_params(sql, *params)
    sql.gsub!(/\?/)  { |a| quote(params.shift) }
  end
end

# Extends ActiveRecord to provide access to the PostgreSQL extenstions for large result sets
class ActiveRecord::Base
  class <<self

    # Ask the database to perform the buffered restult set stream. Pass the cursor name 
    # for the query and the parameters for the #find method. The SQL is built and run.
    def open_cursor(name, *args)
      options = args.last.is_a?(Hash) ? args.pop : {}
      validate_find_options(options)
      set_readonly_option!(options)
      sql = construct_finder_sql(options)
      connection.open_cursor(sql, name)
    end
    
    # Opens a cursor with a full SQL statement and replacable parameters (identified by ? in the SQL)
    def open_cursor_with_sql(name, *sql_and_args, &block)
      connection.cursor_eachrow(sql_and_args, name, &block)
    end

    # Returns a hash[:colname] for the next record in the result set stream for the given cursor name
    def fetch_cursor(name)
      connection.fetch_cursor(name)
    end

    # Closes the result set stream for the named cursor after processing is complete.
    def close_cursor(name)
      connection.close_cursor(name)
    end

    # Like the #find method, this creates a result set stream with the given cursor name.
    # If transaction is true, it will wrap a transaction block around the processing. Each
    # row is yeilded to the block as a hash[:colname] 
    def find_cursor(name, transaction, *findargs)
      connection.begin_db_transaction if transaction
      open_cursor(name, *findargs)
      count = 0
      while (row = ActiveRecord::Base.fetch_cursor(name)) do
        count+= 1
        yield row
      end
      close_cursor(name)
      connection.commit_db_transaction if transaction
      count
    end
    
  end

end
