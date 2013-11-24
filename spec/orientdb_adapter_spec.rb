spec_path = File.dirname(__FILE__)

require File.expand_path("#{spec_path}/spec_helper")
require File.expand_path(File.dirname(__FILE__) + '/../lib/active_record/connection_adapters/orientdb_adapter')

describe ActiveRecord::ConnectionHandling do
  subject do
    class ConnectionHandlingTester
      extend ActiveRecord::ConnectionHandling
    end
  end

  let(:config) do { 
      connection_type: connection_type
    }
  end
  
  describe '#orientdb_connection' do

    context 'when connection type is unknown' do
      let(:connection_type) { 'unknown_type' }
      it 'raises error' do
        expect { subject.orientdb_connection(config) }.to raise_error
      end
    end

  end
end
