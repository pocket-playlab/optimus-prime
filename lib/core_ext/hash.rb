class Hash
  # Code in this file is adapted or inspired by code in the ActiveSupport gem

  def stringify_nested_symbolic_keys
    deep_transform_keys { |key| key.is_a?(Symbol) ? key.to_s : key }
  end

  def symbolize_nested_keys
    deep_transform_keys { |key| key.respond_to?(:to_sym) ? key.to_sym : key }
  end

  def deep_transform_keys(&block)
    _deep_transform_keys_in_object(self, &block)
  end

  private

  def _deep_transform_keys_in_object(object, &block)
    case object
    when Hash
      object.each_with_object({}) do |(key, value), result|
        result[yield(key)] = _deep_transform_keys_in_object(value, &block)
      end
    when Array
      object.map { |el| _deep_transform_keys_in_object(el, &block) }
    else
      object
    end
  end
end
