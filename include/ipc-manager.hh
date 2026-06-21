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

public:
    explicit ipc_manager(std::string_view name) noexcept
        : manager_name_{name}
    {
    }

    virtual ~ipc_manager() = default;

    std::string_view manager_name() const { return manager_name_; }

    ipc_manager(const ipc_manager &) = delete;

    ipc_manager &operator=(const ipc_manager &) = delete;

    ipc_manager(ipc_manager &&) = delete;

    ipc_manager &operator=(ipc_manager &&) = delete;

    void initialize()
    {
        static_cast<derived *>(this)->initialize();
    }

    void run()
    {
        initialize();
        while (!signal_handler::should_exit())
        {
            if (process_messages())
                break;
        }
        cleanup();
    }

    bool process_messages()
    {
        return static_cast<derived *>(this)->process_messages();
    }

    void cleanup()
    {
        static_cast<derived *>(this)->cleanup();
    }
};

#endif