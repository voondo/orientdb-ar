module ActiveRecord
  module Tasks # :nodoc:
    class OrientDBDatabaseTasks # :nodoc:

      delegate :connection, :establish_connection, to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration
      end

      def create
        establish_connection configuration
        binding.pry
        connection.create_database configuration['database'], creation_options
      rescue ActiveRecord::StatementInvalid => error
        binding.pry
        if /database exists/ === error.message
          raise DatabaseAlreadyExists
        else
          raise
        end
      rescue Exception => error
        $stderr.puts "Couldn't create database for #{configuration.inspect}, #{creation_options.inspect}"
        $stderr.puts error
        $stderr.puts error.backtrace
      end

      def drop
        establish_connection configuration
        connection.drop_database configuration['database']
      end

      def purge
        establish_connection :test
        connection.recreate_database configuration['database'], creation_options
      end

      def structure_dump(filename)
        args = prepare_command_options('mysqldump')
        args.concat(["--result-file", "#{filename}"])
        args.concat(["--no-data"])
        args.concat(["#{configuration['database']}"])
        unless Kernel.system(*args)
          $stderr.puts "Could not dump the database structure. "\
                       "Make sure `mysqldump` is in your PATH and check the command output for warnings."
        end
      end

      def structure_load(filename)
        args = prepare_command_options('mysql')
        args.concat(['--execute', %{SET FOREIGN_KEY_CHECKS = 0; SOURCE #{filename}; SET FOREIGN_KEY_CHECKS = 1}])
        args.concat(["--database", "#{configuration['database']}"])
        Kernel.system(*args)
      end

      private

      def configuration
        @configuration
      end

      def configuration_without_database
        configuration.merge('database' => nil)
      end

      def creation_options
        Hash.new.tap do |options|
          #options[:charset]     = configuration['encoding']   if configuration.include? 'encoding'
        end
      end

      def grant_statement
        <<-SQL
GRANT ALL PRIVILEGES ON #{configuration['database']}.*
  TO '#{configuration['username']}'@'localhost'
IDENTIFIED BY '#{configuration['password']}' WITH GRANT OPTION;
        SQL
      end

      def root_configuration_without_database
        configuration_without_database.merge(
          'username' => 'root',
          'password' => root_password
        )
      end

      def root_password
        $stdout.print "Please provide the root password for your mysql installation\n>"
        $stdin.gets.strip
      end

      def prepare_command_options(command)
        args = [command]
        args.concat(['--user', configuration['username']]) if configuration['username']
        args << "--password=#{configuration['password']}"  if configuration['password']
        args.concat(['--default-character-set', configuration['encoding']]) if configuration['encoding']
        configuration.slice('host', 'port', 'socket').each do |k, v|
          args.concat([ "--#{k}", v.to_s ]) if v
        end

        args
      end
    end
  end
end

ActiveRecord::Tasks::DatabaseTasks.register_task(/orientdb/, ActiveRecord::Tasks::OrientDBDatabaseTasks)

