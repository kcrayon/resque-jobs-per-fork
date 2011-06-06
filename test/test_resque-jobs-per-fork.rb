require 'test_helper'

class SomeJob
  def self.perform(i)
    $SEQUENCE << "work_#{i}".to_sym
  end
end

Resque.before_perform_jobs_per_fork do |worker|
  $SEQUENCE << :before_perform_jobs_per_fork
end

Resque.after_perform_jobs_per_fork do |worker|
  $SEQUENCE << :after_perform_jobs_per_fork
end

class TestResqueMultiJobFork < Test::Unit::TestCase
  def setup
    $SEQUENCE = []

    ENV['JOBS_PER_FORK'] = '2'
    @worker = Resque::Worker.new(:jobs)
    @worker.cant_fork = true
  end

  def test_fewer_jobs_than_per_fork_limit
    Resque::Job.create(:jobs, SomeJob, 1)
    @worker.work(0)

    assert_equal([:before_perform_jobs_per_fork, :work_1, :after_perform_jobs_per_fork], $SEQUENCE)
  end

  def test_same_number_of_jobs_as_per_fork_limit
    Resque::Job.create(:jobs, SomeJob, 1)
    Resque::Job.create(:jobs, SomeJob, 2)
    @worker.work(0)

    assert_equal([:before_perform_jobs_per_fork, :work_1, :work_2, :after_perform_jobs_per_fork], $SEQUENCE)
  end

  def test_more_jobs_than_per_fork_limit
    Resque::Job.create(:jobs, SomeJob, 1)
    Resque::Job.create(:jobs, SomeJob, 2)
    Resque::Job.create(:jobs, SomeJob, 3)
    @worker.work(0)

    assert_equal([
       :before_perform_jobs_per_fork, :work_1, :work_2, :after_perform_jobs_per_fork,
       :before_perform_jobs_per_fork, :work_3, :after_perform_jobs_per_fork
    ], $SEQUENCE)
  end

  def test_should_default_to_one_job_per_fork_if_env_not_set
    ENV.delete('JOBS_PER_FORK')

    assert_nothing_raised(RuntimeError) do
      Resque::Job.create(:jobs, SomeJob, 1)
      Resque::Job.create(:jobs, SomeJob, 2)
      @worker.work(0)

      assert_equal([
         :before_perform_jobs_per_fork, :work_1, :after_perform_jobs_per_fork,
         :before_perform_jobs_per_fork, :work_2, :after_perform_jobs_per_fork
      ], $SEQUENCE)
    end
  end
end
