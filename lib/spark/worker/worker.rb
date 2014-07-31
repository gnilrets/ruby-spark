#!/usr/bin/env ruby

# =================================================================================================
# Worker
#
module Worker
  class Base
    include Spark::Serializer::Helper

    attr_accessor :client_socket

    def initialize(client_socket)
      self.client_socket = client_socket
    end

    def run
      before_start

      load_split_index
      load_command
      load_iterator

      compute

      send_result

      before_end
    end

    private

      def before_start
      end

      def before_end
        client_socket.write(pack_int(0))
        client_socket.flush

        # loop { break if client_socket.recv(4096) == '' }
      end

      def read(size)
        client_socket.read(size)
      end

      def read_int
        unpack_int(read(4))
      end

      def load_split_index
        @split_index = read_int
      end

      def load_command
        @command = Marshal.load(read(read_int))
      end

      def load_iterator
        @iterator = @command.deserializer.load(client_socket)
      end

      def compute
        begin
          @command.library.each{|lib| require lib}
          @command.pre.each{|pre| eval(pre)}

          @command.stages.each do |stage|
            eval(stage.pre)
            @iterator = eval(stage.main).call(@iterator, @split_index)
          end
        rescue => e
          client_socket.write(pack_int(-1))
          client_socket.write(pack_int(e.message.size))
          client_socket.write(e.message)

          # Thread.kill
        end
      end

      def send_result
        @command.serializer.dump(@iterator, client_socket)
      end

  end

  # ===============================================================================================
  # Worker::Process
  #
  class Process < Base
    private
    
      def before_start
        $PROGRAM_NAME = "RubySparkWorker"

        Signal.trap("HUP", "DEFAULT")
        Signal.trap("CHLD", "DEFAULT")
      end
  end

  # ===============================================================================================
  # Worker::Thread
  #
  class Thread < Base
    private
    
      # Threads changing is very slow
      # Faster way is do it one by one
      def load_iterator
        $mutex.synchronize{ super }
      end
  end

end
