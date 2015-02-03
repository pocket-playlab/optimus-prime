module OptimusPrime
  class Transform
  	attr_accessor :sources

  	def initialize(sources)
  	  raise "sources is required" unless sources

  	  raise "sources must be an array" unless source.kind_of? Array

  	  # validate that source "is a" OptimusPrime::Source OR OptimusPrime::Transform
  	  source.each do |source|
	    unless (source.is_a? OptimusPrime::Source or source.is_a? OptimusPrime::Transform)
	  	  raise "source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
	  	end
	  end

	end
  end
end
