$LOAD_PATH.unshift(File.dirname(__FILE__))

module OrientDB
  module AR
  end
end

require 'orientdb-ar/document_mixin'
require 'orientdb-ar/base'
require 'orientdb-ar/embedded'
require 'orientdb-ar/ext'

require 'active_record/connection_adapters/orientdb_adapter'
require 'active_record/tasks/orientdb_database_tasks'

OrientDB::SQL.monkey_patch!
