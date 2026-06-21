#ifndef _IPC_MANAGER_H_
#define _IPC_MANAGER_H_

#include <string_view>

template <typename derived>
class ipc_manager
{
protected:
    int counter_{0};
    std::string_view manager_name_;

public:
    explicit ipc_manager(std::string_view name) : manager_name_{name}
    {
    }

    virtual ~ipc_manager() = default;

    std::string_view manager_name() const { return manager_name_; }

    // void run()
    // {
    //     auto &current_mgr = static_cast<derived &>(*this);

    //     current_mgr.process_messages();
    // }

    void initialize()
    {
        static_cast<derived *>(this)->initialize();
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