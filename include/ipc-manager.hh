#ifndef _IPC_MANAGER_H_
#define _IPC_MANAGER_H_

#include <string_view>

#include "signal-handler.hh"

template <typename pipe_manager>
class ipc_manager
{
protected:
    int counter_{0};
    std::string_view manager_name_;
    syslog_logger logger_;
    bool is_producer_;
    bool is_initialized_{false};

    int writer_fd_{-1};
    int reader_fd_{-1};

    static constexpr const char *PIPE_TO_RECEIVER = "/tmp/counter_to_receiver";
    static constexpr const char *PIPE_TO_SENDER = "/tmp/counter_to_sender";
    static constexpr int MAX_COUNTER{10};
    static constexpr auto POLL_INTERVAL{std::chrono::milliseconds(100)};
    static constexpr auto PROCESS_INTERVAL{std::chrono::milliseconds(500)};
    static constexpr auto CONNECTION_TIMEOUT{std::chrono::seconds(30)};

public:
    explicit ipc_manager(std::string_view name, bool is_producer) noexcept
        : manager_name_{name}, logger_{name}, is_producer_{is_producer}
    {
    }

    virtual ~ipc_manager() = default;

    std::string_view manager_name() const { return manager_name_; }

    ipc_manager(const ipc_manager &) = delete;

    ipc_manager &operator=(const ipc_manager &) = delete;

    ipc_manager(ipc_manager &&) = delete;

    ipc_manager &operator=(ipc_manager &&) = delete;

    void run()
    {
        logger_.notice("Run loop starting");

        initialize();
        while (!signal_handler::should_exit())
        {
            if (process_messages())
                break;
        }
        cleanup();
        logger_.notice(std::string("Run loop finished. Final counter" + std::to_string(counter_)));
    }

protected:
    void initialize()
    {
        try
        {
            logger_.info("Starting initialization");
            static_cast<pipe_manager *>(this)->initialize();
        }
        catch (const std::exception &e)
        {
            logger_.error(std::string("Initialization failed: ") + e.what());
            throw;
        }
        logger_.info("Initialization completed");
    }

    bool process_messages()
    {
        return static_cast<pipe_manager *>(this)->process_messages();
    }

    void cleanup()
    {
        logger_.info("Starting cleanup");
        static_cast<pipe_manager *>(this)->cleanup();
    }
};

#endif