require 'resque'
require 'resque/worker'

module Resque

  # the `before_perform_jobs_per_fork` hook will run in the child perform
  # right before the child perform starts
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.before_perform_jobs_per_fork(&block)
    block ? (@before_perform_jobs_per_fork = block) : @before_perform_jobs_per_fork
  end

  # Set the before_perform_jobs_per_fork proc.
  def self.before_perform_jobs_per_fork=(before_perform_jobs_per_fork)
    @before_perform_jobs_per_fork = before_perform_jobs_per_fork
  end

  # the `after_perform_jobs_per_fork` hook will run in the child perform
  # right before the child perform terminates
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.after_perform_jobs_per_fork(&block)
    block ? (@after_perform_jobs_per_fork = block) : @after_perform_jobs_per_fork
  end

  # Set the after_perform_jobs_per_fork proc.
  def self.after_perform_jobs_per_fork=(after_perform_jobs_per_fork)
    @after_perform_jobs_per_fork = after_perform_jobs_per_fork
  end

  class Worker

    def perform_with_jobs_per_fork(job)
      raise "You need to set JOBS_PER_FORK on the command line" unless ENV['JOBS_PER_FORK']

      run_hook :before_perform_jobs_per_fork, self

      ENV['JOBS_PER_FORK'].to_i.times do |attempts|
        break if shutdown?

        if attempts > 0
          # Attempt to reserve another job
          job = reserve

          if job
            working_on job
            procline "Processing #{job.queue} since #{Time.now.to_i}"
          end
        end

        if job
          perform_without_jobs_per_fork(job)
          processed!
        end
      end

      run_hook :after_perform_jobs_per_fork, self
    end

    alias_method :perform_without_jobs_per_fork, :perform
    alias_method :perform, :perform_with_jobs_per_fork
  end
end
