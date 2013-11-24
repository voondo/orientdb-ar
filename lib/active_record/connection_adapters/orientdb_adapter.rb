module ActiveRecord
  module ConnectionHandling

    def orientdb_connection(config)
      puts config
      config = config.symbolize_keys
      connection_type = config[:connection_type]
      case connection_type
      when :remote
        server_admin = OrientDB::ServerAdmin('remote:0.0.0.0')
        server_admin.connect(username, password)
      when :local
        raise 'Local not yet implemented.'
      else
        raise "Unknown connection_type: #{connection_type}"
      end

    end
  end
end
