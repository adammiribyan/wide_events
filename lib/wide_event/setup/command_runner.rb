require "open3"

module WideEvent
  module Setup
    # Runs one external command (Kamal, in practice) via Open3.capture3 and
    # reduces it to the three things WideEvent::Setup::Checker actually
    # needs: whether it succeeded, and its stdout/stderr text. `argv` is
    # always an array of literal strings (the executable and its arguments,
    # never a single shell string), so no argument is ever re-interpreted by
    # a shell. This is the only piece of Checker that ever shells out; every
    # test replaces it with a fake that returns canned Results instead.
    class CommandRunner
      Result = Struct.new(:stdout, :stderr, :success, keyword_init: true) do
        def success?
          !!success
        end
      end

      def call(argv)
        stdout, stderr, status = Open3.capture3(*argv)
        Result.new(stdout: stdout, stderr: stderr, success: status.success?)
      end
    end
  end
end
