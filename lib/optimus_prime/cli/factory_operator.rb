require_relative 'optimus_operator'

module OptimusPrime
  module CLI
    # This class wraps and runs a factory config file
    class FactoryOperator < OptimusOperator
      def operate
        config.each do |pl|
          if can_run?(pl)
            exit_statuses[pl['id']] = activate(pl)
          else
            puts "Not running #{pl['id']} because one of its dependencies didn't exit successfully."
            exit_statuses[pl['id']] = false
          end
        end
        puts 'Factory finished.'
      end

      def exit_statuses
        @exit_statuses ||= {}
      end

      private

      def can_run?(pl)
        return true unless pl['dependencies']
        pl['dependencies'].each do |dep|
          return false unless exit_statuses[dep].zero?
        end
        true
      end

      def activate(pl)
        puts "Starting pipeline #{pl['id']} from #{pl['file']} named #{pl['pipeline']}"
        sleep pl['pre_wait'] if pl['pre_wait']
        system("bundle exec optimus operate pipeline #{pl['file']} #{pl['pipeline']}",
               out: $stdout,
               err: $stderr
        )
        exitstatus = $CHILD_STATUS.exitstatus # also $? would work
        puts "#{pl['id']} finished with exit code #{exitstatus}"
        sleep pl['post_wait'] if pl['post_wait']
        exitstatus
      end
    end
  end
end
