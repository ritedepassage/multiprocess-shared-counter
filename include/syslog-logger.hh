#pragma once

#include "logger.hh"
#include <syslog.h>
#include <string>
#include <memory>

class syslog_connection
{
    std::string ident_;
    bool is_open_{false};

public:
    explicit syslog_connection(std::string_view ident,
                               int option = LOG_PID | LOG_NDELAY,
                               int facility = LOG_USER)
        : ident_(ident)
    {
        openlog(ident_.c_str(), option, facility);
        is_open_ = true;
    }

    ~syslog_connection()
    {
        if (is_open_)
        {
            closelog();
        }
    }

    syslog_connection(const syslog_connection &) = delete;
    syslog_connection &operator=(const syslog_connection &) = delete;
    syslog_connection(syslog_connection &&) = delete;
    syslog_connection &operator=(syslog_connection &&) = delete;

    void write(int priority, std::string_view message) const
    {
        if (is_open_)
        {
            syslog(priority, "%.*s",
                   static_cast<int>(message.size()), message.data());
        }
    }
};

// Syslog logger using CRTP
class syslog_logger : public logger<syslog_logger>
{
private:
    static inline std::unique_ptr<syslog_connection> connection_{nullptr};
    std::string component_name_;

    static int to_syslog_priority(log_level level) noexcept
    {
        switch (level)
        {
        case log_level::DEBUG:
            return LOG_DEBUG;
        case log_level::INFO:
            return LOG_INFO;
        case log_level::NOTICE:
            return LOG_NOTICE;
        case log_level::WARNING:
            return LOG_WARNING;
        case log_level::ERROR:
            return LOG_ERR;
        case log_level::CRIT:
            return LOG_CRIT;
        }
        return LOG_DEBUG;
    }

public:
    static void init(std::string_view app_name, int facility = LOG_USER)
    {
        if (!connection_)
        {
            connection_ = std::make_unique<syslog_connection>(
                app_name, LOG_PID | LOG_NDELAY, facility);
        }
    }

    static void shutdown()
    {
        connection_.reset();
    }

    explicit syslog_logger(std::string_view component_name)
        : component_name_(component_name) {}

    void log_impl(log_level level, std::string_view message) const
    {
        if (!connection_)
            return;

        auto formatted = "[" + component_name_ + "] " + std::string(message);
        connection_->write(to_syslog_priority(level), formatted);
    }
};