require 'orientdb'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'

module ActiveRecord

  # FIXME dirty monkey patch to get stuff work
  class Migrator
    def self.needs_migration?
      false
    end
  end

  module ConnectionHandling

    VALID_ORIENTDB_CONN_PARAMS = %i( connection host password username )
    ORIENTDB_DATABASE_TYPES = %w( graph document )

    attr_accessor :server_admin

    def orientdb_connection(config)
      ConnectionAdapters::OrientDBAdapter.new config
    end
  end

  module ConnectionAdapters

    class OrientDBColumn < Column
    end


    class OrientDBAdapter < AbstractAdapter
      class ColumnDefinition < ActiveRecord::ConnectionAdapters::ColumnDefinition
      end

      module ColumnMethods
      end

      class TableDefinition < ActiveRecord::ConnectionAdapters::ColumnDefinition
        include ColumnMethods

        def primary_key(name, type = :primary_key, options = {})
          raise 'Not implemented'
        end

        def column(name, type = nil, options = {})
          raise 'Not implemented'
        end

        private
        def create_column_definition(name, type)
          ColumnDefinition.new(name, type)
        end
      end

      class Table < ActiveRecord::ConnectionAdapters::Table
        include ColumnMethods
      end

      ADAPTER_NAME = 'OrientDB'

      NATIVE_DATABASE_TYPES = { }

      #### ActiveRecord Mixins
      # include Quoting
      # include ReferentialIntegrity
      # include SchemaStatements
      # include DatabaseStatements
      # include Savepoints

      def adapter_name
        ADAPTER_NAME
      end

      def supports_migrations?
         false
      end

      def tables
        @connection.get_cluster_names
      end

      def active?
        !@connection.is_closed
      end

      def disconnect!
        @connection.close
        super
      end
      
      class StatementPool < ConnectionAdapters::StatementPool
      end

      def initialize config

        config.symbolize_keys!
        connection_type = config[:connection].to_sym
        database = config[:database]
        database.symbolize_keys!
        username = config[:username]
        password = config[:password]

        case connection_type

        when :remote then
          load_server_admin(host)
          remote_login(username, password)

          database[:url] = database_url connection_type, database[:host], database[:name]

        when :local then
          database[:url] = "local:#{database[:path]}"

        else
          raise "Unknown connection type: #{connection[:type]}"
        end
        connection = get_connection(connection_type, database, username, password)
        @connection_type = connection_type

        super connection
      end


      def create_database(database, opts = {})
        case @connection_type
        when :remote then
          @server_admin.create_database(database[:name], database[:type], database[:storage])
        when :local then
          @connection.create
        end
      end

      private
      def load_server_admin(host)
        @server_admin = OrientDB::CLIENT::remote::OServerAdmin.new("remote:#{host}")
      end

      def remote_login(username, password)
        @server_admin.connect(username, password)
      end

      def find_or_create_database(database)
        return locate_database(database) || create_database(database)
      end

      def locate_database(database)
        database.symbolize_keys
        return @server_admin.list_databases[database[:name].to_s]
      end

      def get_connection(connection_type, database, username, password)
        # TODO: support different connection types ( pooled, nonpooled, etc. -
        # see OrientDB JRuby spec_helper )
        case database[:type].to_sym
        when :graph then
          case connection_type
          when :local then
            #OrientDB::OrientGraph.new database[:url], username, password
            raise "Unimplemented yet"
          else
            raise "Unimplemented yet"
          end

        when :document then
          case connection_type
          when :remote then
            OrientDB::DocumentDatabasePool.new database[:url], username, password
          when :local then
            db = OrientDB::DocumentDatabase.new database[:url]
            db.open username, password if db.exists
            db
          end

        else
          raise "Unknown database type : #{database[:type]}"
        end
      end

      def database_url(connection, host, database_name)
        return "#{connection}:#{host}/#{database_name}"
      end
    end

  end
end
