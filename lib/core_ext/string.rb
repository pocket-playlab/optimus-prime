class String
  def convert_to(data_type)
    data_type = data_type.capitalize
    if ['Integer', 'Float'].include? data_type
      send data_type, self
    elsif data_type == 'Boolean'
      self == 'true'
    else
      self
    end
  end
end