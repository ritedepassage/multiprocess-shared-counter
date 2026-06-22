#ifndef _IPC_MANAGER_H_
#define _IPC_MANAGER_H_

#include <string_view>

#include "signal-handler.hh"

template <typename derived>
class ipc_manager
{
protected:
    int counter_{0};
    std::string_view manager_name_;
    syslog_logger logger_;

public:
    explicit ipc_manager(std::string_view name) noexcept
        : manager_name_{name}, logger_{name}
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
            static_cast<derived *>(this)->initialize();
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
        return static_cast<derived *>(this)->process_messages();
    }

    void cleanup()
    {
        logger_.info("Starting cleanup");
        static_cast<derived *>(this)->cleanup();
    }
};

#endif