spec_path = File.dirname(__FILE__)

require File.expand_path("#{spec_path}/spec_helper")
require File.expand_path(File.dirname(__FILE__) + '/../lib/active_record/connection_adapters/orientdb_adapter')

describe ActiveRecord::ConnectionHandling do
  subject do
    class ConnectionHandlingTester
      extend ActiveRecord::ConnectionHandling
    end
  end

  let(:base_config) do {
      connection: connection_type,
      username: 'admin',
      password: 'admin'
    }
  end
  let(:config) { base_config.merge(example_config) }
  
  describe '#orientdb_connection' do

    context 'when connection type is unknown' do
      let(:connection_type) { 'unknown_type' }
      let(:example_config) { { } }
      it 'raises error' do
        expect { subject.orientdb_connection(config) }.to raise_error(RuntimeError)
      end
    end

    context 'when connection type is :local' do
      let(:connection_type) { :local }
      let(:example_config) { { } }
      it 'raises error - not implemented' do
        expect { subject.orientdb_connection(config) }.to raise_error(RuntimeError)
      end
    end

    context 'when connection type is :remote' do
      let(:connection_type) { :remote }
      let(:database_type) { 'graph' }

      let(:database) do {
          name: 'linkstreme-test',
          type: database_type,
          storage: 'plocal'
        }
      end

      let(:example_config) do {
          host:     '0.0.0.0',
          database: database
        }
      end

      it 'creates ServerAdmin' do
        OrientDB::CLIENT::remote::OServerAdmin.should_receive(:new).and_call_original()
        subject.orientdb_connection(config)
      end

      context 'when database[:type] = "graph"' do
        let(:database_type) { 'graph' }

        it 'returns OrientDB::OrientGraph object' do
          pending 'Not yet implemented.'
          subject.orientdb_connection(config).class.
            should be OrientDB::OrientGraph
        end

        it 'returns OrientDB::DocumentDatabase object' do
          subject.orientdb_connection(config).class.
            should be OrientDB::DocumentDatabasePooled
        end
      end

      context 'when database[:type] = "document"' do
        let(:database_type) { 'document' }

        it 'returns OrientDB::DocumentDatabase object' do
          subject.orientdb_connection(config).class.
            should be OrientDB::DocumentDatabasePooled
        end
      end

      describe '#create_database' do
        context 'when database[:type] is unknown' do
          let(:database_type) { 'unknown_database_type' }

          it 'raises an error' do
            expect { connection = subject.send(:create_database, database) }.to raise_error
          end
        end
      end

      context 'when pooled connection is requested' do
        it 'returns pooled connection'
      end

    end

  end
end
