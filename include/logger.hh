#pragma once

#include <string>
#include <string_view>
#include <type_traits>

enum class log_level : int
{
    DEBUG = 0,
    INFO = 1,
    NOTICE = 2,
    WARNING = 3,
    ERROR = 4,
    CRIT = 5
};

// CRTP base class for loggers - static polymorphism
template <typename derived>
class logger
{
public:

    logger() = default;
    virtual ~logger() = default;

    void debug(std::string_view message) const
    {
        log(log_level::DEBUG, message);
    }

    void info(std::string_view message) const
    {
        log(log_level::INFO, message);
    }

    void notice(std::string_view message) const
    {
        log(log_level::NOTICE, message);
    }

    void warning(std::string_view message) const
    {
        log(log_level::WARNING, message);
    }

    void error(std::string_view message) const
    {
        log(log_level::ERROR, message);
    }

    void critical(std::string_view message) const
    {
        log(log_level::CRIT, message);
    }

protected:
    void log(log_level level, std::string_view message) const
    {
        static_cast<const derived *>(this)->log_impl(level, message);
    }
};
