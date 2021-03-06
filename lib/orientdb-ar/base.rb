class OrientDB::AR::Base

  include ActiveModel::AttributeMethods
  include OrientDB::AR::DocumentMixin
  include ActiveModel::Callbacks

  class_attribute :connection

  define_callbacks :validation, :save, :delete

  def save(perform_validations = true)
    run_callbacks :save do
      if perform_validations
        validate
        if @last_validation_result
          save_without_validations
        else
          false
        end
      else
        save_without_validations
      end
    end
  end

  def save_without_validations
    @odocument.save
    @saved              = true
    @previously_changed = @changed_attributes
    @changed_attributes.clear
    true
  end

  private :save_without_validations

  def delete
    run_callbacks :delete do
      @odocument.delete
      @deleted = true
    end
    true
  end

  def reload
    raise "Not persisted, cannot reload" unless persisted?
    @odocument          = OrientDB::AR::Query.new(self.class).where('@rid' => rid.lit).first_result
    @changed_attributes = { }
    @errors             = ActiveModel::Errors.new(self)
    self
  end

  def saved?
    @saved || @odocument.rid != '-1:-1'
  end

  def deleted?
    @deleted ||= false
  end

  def persisted?
    saved? && !deleted?
  end

  class << self

    include OrientDB::AR::Relations

    attr_writer :oclass_name

    def embeddable?
      false
    end

    def oclass
      unless defined?(@oclass)
        options = { }
        unless descends_from_base?
          super_oclass          = superclass.oclass
          options[:super]       = super_oclass
          options[:use_cluster] = super_oclass.cluster_ids.first
        end
        @oclass = connection.get_or_create_class oclass_name, options
      end
      @oclass
    end

    def field(name, type, options = { })
      name = name.to_sym
      if fields.key? name
        puts "Already defined field [#{name}]"
      else
        fields[name] = { :type => type }.update options
      end
    end

    def descends_from_base?
      superclass && superclass == OrientDB::AR::Base
    end

    def create(fields = { })
      obj = new fields
      obj.save
      obj
    end

    def select(*args)
      OrientDB::AR::Query.new(self).select(*args)
    end

    alias :columns :select

    def where(*args)
      OrientDB::AR::Query.new(self).where(*args)
    end

    def order(*args)
      OrientDB::AR::Query.new(self).order(*args)
    end

    def limit(max_records)
      OrientDB::AR::Query.new(self).limit(max_records)
    end

    def range(lower_rid, upper_rid = nil)
      OrientDB::AR::Query.new(self).range(lower_rid, upper_rid)
    end

    def all(conditions = { })
      OrientDB::AR::Query.new(self).where(conditions).all
    end

    def first(conditions = { })
      OrientDB::AR::Query.new(self).where(conditions).first
    end

    def update(*args)
      OrientDB::AR::Update.new(self).values(*args).run
    end

    def delete(*args)
      OrientDB::AR::Delete.new(self).where(*args).run
    end

    def insert(*args)
      from_orientdb OrientDB::AR::Insert.new(self).fields(*args).run
    end

    def count
      oclass.count
    end

    def clear
      oclass.truncate
    end


  end
end

OrientDB::AR::Base.include_root_in_json = false
