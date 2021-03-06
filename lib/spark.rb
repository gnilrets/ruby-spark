# Gems and libraries
require 'method_source'
require 'forwardable'
require 'sourcify'
require 'socket'
require 'tempfile'
require 'tmpdir'

module Spark
  autoload :Context,        'spark/context'
  autoload :Config,         'spark/config'
  autoload :RDD,            'spark/rdd'
  autoload :CLI,            'spark/cli'
  autoload :Build,          'spark/build'
  autoload :Serializer,     'spark/serializer'
  autoload :Helper,         'spark/helper'
  autoload :StorageLevel,   'spark/storage_level'
  autoload :Command,        'spark/command'
  autoload :CommandBuilder, 'spark/command_builder'
  autoload :Sampler,        'spark/sampler'
  autoload :Logger,         'spark/logger'
  autoload :JavaBridge,     'spark/java_bridge'
  autoload :ExternalSorter, 'spark/sort'
  autoload :Constant,       'spark/constant'
  autoload :Broadcast,      'spark/broadcast'
  autoload :Accumulator,    'spark/accumulator'
  autoload :StatCounter,    'spark/stat_counter'
  autoload :Mllib,          'spark/mllib'

  include Helper::System

  def self.print_logo(message=nil)
    puts <<-STRING

    Welcome to
                  __           ____              __
        ______ __/ /  __ __   / __/__  ___ _____/ /__
       / __/ // / _ \\/ // /  _\\ \\/ _ \\/ _ `/ __/  '_/
      /_/  \\_,_/_.__/\\_, /  /___/ .__/\\_,_/_/ /_/\\_\\   version #{Spark::VERSION}
                    /___/      /_/

    #{message}

    STRING
  end

  # Returns current configuration. Configurations can be changed until
  # context is initialized. In this case config is locked only for reading.
  #
  # == Configuration can be changed:
  #
  #   Spark.config.set('spark.app.name', 'RubySpark')
  #
  #   Spark.config['spark.app.name'] = 'RubySpark'
  #
  #   Spark.config do
  #     set 'spark.app.name', 'RubySpark'
  #   end
  #
  def self.config(&block)
    @config ||= Spark::Config.new

    if block_given?
      @config.instance_eval(&block)
    else
      @config
    end
  end

  # Destroy current configuration. This can be useful for restarting config
  # to set new. It has no effect if context is already started.
  def self.clear_config
    @config = nil
  end

  # Return a current active context or nil.
  #
  # TODO: Run `start` if context is nil?
  #
  def self.context
    @context
  end

  # Initialize spark context if not already. Config will be automatically
  # loaded on constructor. From that point `config` will use configuration
  # from running Spark and will be locked only for reading.
  def self.start
    if started?
      # Already started
    else
      @context ||= Spark::Context.new
    end
  end

  def self.stop
    @context.stop
    RubyWorker.stopServer
    logger.info('Workers were stopped')
  rescue
    nil
  ensure
    @context = nil
    clear_config
  end

  def self.started?
    !!@context
  end

  def self.logger
    @logger ||= Spark::Logger.new
  end

  # Root of the gem
  def self.root
    @root ||= File.expand_path('..', File.dirname(__FILE__))
  end

  def self.home
    root
  end

  # Default directory for java extensions
  def self.target_dir
    @target_dir ||= File.join(root, 'target')
  end

  # Directory where is worker.rb
  def self.worker_dir
    @worker_dir ||= File.join(root, 'lib', 'spark', 'worker')
  end

  def self.ruby_spark_jar
    @ruby_spark_jar ||= File.join(target_dir, 'ruby-spark.jar')
  end

  def self.spark_ext_dir
    @spark_ext_dir ||= File.join(root, 'ext', 'spark')
  end


  # ===============================================================================
  # Load JVM and jars

  # Load dependent libraries, can be use once
  # Cannot load before CLI::install
  #
  # == Parameters:
  # spark_home::
  #   path to directory where are located sparks .jar files or single Spark jar
  #
  def self.load_lib(spark_home=nil)
    return if @java_bridge

    spark_home ||= Spark.target_dir

    bridge = JavaBridge.get
    @java_bridge = bridge.new(spark_home)
    @java_bridge.load
    nil
  end

  def self.java_bridge
    @java_bridge
  end


  # Aliases
  class << self
    alias_method :sc, :context
    alias_method :jb, :java_bridge
  end

end

# C/Java extensions
require 'ruby_spark_ext'

# Ruby core extensions
require 'spark/ext/module'
require 'spark/ext/object'
require 'spark/ext/hash'
require 'spark/ext/string'
require 'spark/ext/integer'
require 'spark/ext/ip_socket'
require 'spark/ext/io'

# Other requirments
require 'spark/version'
require 'spark/error'

# Make sure that Spark be always stopped
Kernel.at_exit do
  begin
    Spark.started? && Spark.stop
  rescue
  end
end
