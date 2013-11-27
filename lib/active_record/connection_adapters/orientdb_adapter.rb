require 'orientdb'

module ActiveRecord
  module ConnectionHandling

    DATABASE_TYPES = %w{ graph document }

    attr_accessor :server_admin

    def orientdb_connection(config)
      config = config.symbolize_keys

      connection = config[:connection]
      host       = config[:host]
      username   = config[:username]
      password   = config[:password]
      database   = config[:database]

      case connection.to_sym
      when :remote
        load_server_admin(host)
        remote_login(username, password)

        if find_or_create_database(database)
          database[:url] = database_url(connection, host, database[:name])
          return get_connection(database, username, password)
        else
          raise "Could not find or create database: #{database}"
        end

      when :local
        raise 'Local connections not yet implemented.'

      else
        raise "Unknown connection_type: #{connection}"
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

    def create_database(database)
      raise "Unknown database type: #{database[:type]}" unless
        DATABASE_TYPES.member? database[:type]

      @server_admin.create_database(database[:name], database[:type], database[:storage])
      return locate_database(database)
    end

    def get_connection(database, username, password)
      # TODO: eventually want to be able to return a OrientGraph object for 'graph' type
      # TODO: support different connection types ( pooled, nonpooled, etc. -
      # see OrientDB JRuby spec_helper )
      return OrientDB::DocumentDatabasePool.connect(database[:url], username, password)
    end

    def database_url(connection, host, database_name)
      return "#{connection}:#{host}/#{database_name}"
    end

  end
end
