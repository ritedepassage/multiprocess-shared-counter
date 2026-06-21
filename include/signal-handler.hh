#ifndef _SIGNAL_HANDLER_H_
#define _SIGNAL_HANDLER_H_

#include <atomic>
#include <type_traits>
#include <atomic>
#include <csignal>

class signal_handler
{
private:
    static inline std::atomic<bool> exit_flag_{false};

public:
    static void setup(std::initializer_list<int> signals = {SIGINT, SIGTERM})
    {
        struct sigaction applied_sig_action{};

        applied_sig_action.sa_handler = handle_signal;
        sigisemptyset(&applied_sig_action.sa_mask);

        for (int sig_val : signals)
        {
            sigaction(sig_val, &applied_sig_action, nullptr);
        }
    }

    static bool should_exit() noexcept
    {
        return exit_flag_.load(std::memory_order_relaxed);
    }

    static void request_exit() noexcept
    {
        exit_flag_.store(true, std::memory_order_relaxed);
    }

private:
    static void handle_signal(int) noexcept
    {
        exit_flag_.store(true, std::memory_order_relaxed);
    }
};

#endif