#pragma once

#include <sys/epoll.h>
#include <cstring>
#include <system_error>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <thread>

#include <sstream>

#include "ipc-manager.hh"

class nonblocking_pipe_manager : public ipc_manager<nonblocking_pipe_manager>
{
private:
    int epoll_fd_{-1};

    static constexpr auto EPOLL_TIMEOUT{std::chrono::seconds(10)};
    static constexpr int MAX_EPOLL_EVENTS = 2;

public:
    explicit nonblocking_pipe_manager(bool is_producer) : ipc_manager{"nonblocking_pipe", is_producer}
    {
    }

    ~nonblocking_pipe_manager() override
    {
        cleanup();
    }

    void initialize()
    {
        epoll_fd_ = epoll_create(EPOLL_CLOEXEC);

        if (epoll_fd_ == -1)
        {
            logger_.error(std::string("Failed to create epoll: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to create epoll instance");
        }

        logger_.debug("Epoll instance created");

        if (is_producer_)
        {
            setup_as_producer();
        }
        else
        {
            setup_as_consumer();
        }

        is_initialized_ = true;
    }

    bool process_messages()
    {
        if (!is_initialized_)
        {
            logger_.error("Process messages called before initialization");
            return true;
        }
        return is_producer_ ? process_as_producer() : process_as_consumer();
    }

    void cleanup() noexcept
    {
        if (!is_initialized_)
        {
            return; // Nothing to cleanup
        }

        logger_.debug("Cleaning up epoll and file descriptors");

        // Close epoll first
        if (epoll_fd_ >= 0)
        {
            close(epoll_fd_);
            epoll_fd_ = -1;
            logger_.debug("Epoll instance closed");
        }

        // Close pipe file descriptors
        if (writer_fd_ >= 0)
        {
            close(writer_fd_);
            writer_fd_ = -1;
            logger_.debug("Writer fd closed");
        }
        if (reader_fd_ >= 0)
        {
            close(reader_fd_);
            reader_fd_ = -1;
            logger_.debug("Reader fd closed");
        }

        // Remove pipe files (only producer created them)
        if (is_producer_)
        {
            unlink(PIPE_TO_RECEIVER);
            unlink(PIPE_TO_SENDER);
            logger_.debug("Removed pipe files");
        }

        is_initialized_ = false;
        logger_.debug("Cleanup complete");
    }

private:
    void add_to_epoll(int fd, unsigned events)
    {
        struct epoll_event ev;
        ev.events = events;
        ev.data.fd = fd;

        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1)
        {
            logger_.error(std::string("Failed to add fd to epoll: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to add fd to epoll");
        }
    }

    void setup_as_producer()
    {
        // Remove stale pipes
        unlink(PIPE_TO_RECEIVER);
        unlink(PIPE_TO_SENDER);

        // Create pipes
        if (mkfifo(PIPE_TO_RECEIVER, 0666) == -1)
        {
            logger_.error(std::string("Failed to create pipe: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to create receiver pipe");
        }
        logger_.debug("Created receiver pipe");

        if (mkfifo(PIPE_TO_SENDER, 0666) == -1)
        {
            unlink(PIPE_TO_RECEIVER);
            logger_.error(std::string("Failed to create pipe: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to create sender pipe");
        }

        logger_.info("Waiting for consumer to connect (epoll)...");

        auto start = std::chrono::steady_clock::now();
        while ((writer_fd_ = open(PIPE_TO_RECEIVER, O_WRONLY | O_NONBLOCK)) == -1)
        {
            if (errno == ENXIO)
            {
                // No reader yet - expected, retry
                auto elapsed = std::chrono::steady_clock::now() - start;
                if (elapsed > CONNECTION_TIMEOUT)
                {
                    logger_.error("Timeout waiting for consumer to connect");
                    throw std::runtime_error("Timeout waiting for consumer");
                }

                if (signal_handler::should_exit())
                {
                    throw std::runtime_error("Interrupted while waiting");
                }

                std::this_thread::sleep_for(POLL_INTERVAL);
                continue;
            }

            // Real error
            logger_.error(std::string("Failed to open write pipe: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open write pipe");
        }

        add_to_epoll(writer_fd_, EPOLLOUT);

        start = std::chrono::steady_clock::now();
        while ((reader_fd_ = open(PIPE_TO_SENDER, O_RDONLY | O_NONBLOCK)) == -1)
        {
            if (errno == ENXIO)
            {
                auto elapsed = std::chrono::steady_clock::now() - start;
                if (elapsed > CONNECTION_TIMEOUT)
                {
                    close(writer_fd_);
                    writer_fd_ = -1;
                    logger_.error("Timeout waiting for consumer read pipe");
                    throw std::runtime_error("Timeout waiting for consumer");
                }

                if (signal_handler::should_exit())
                {
                    close(writer_fd_);
                    throw std::runtime_error("Interrupted while waiting");
                }

                std::this_thread::sleep_for(POLL_INTERVAL);
                continue;
            }

            close(writer_fd_);
            writer_fd_ = -1;
            logger_.error(std::string("Failed to open read pipe: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open read pipe");
        }

        add_to_epoll(reader_fd_, EPOLLIN);
    }

    void setup_as_consumer()
    {
        logger_.info("Waiting for producer to create pipes...");

        // Wait for pipes to exist
        auto start = std::chrono::steady_clock::now();
        while (access(PIPE_TO_RECEIVER, F_OK) == -1 ||
               access(PIPE_TO_SENDER, F_OK) == -1)
        {

            if (signal_handler::should_exit())
            {
                throw std::runtime_error("Interrupted while waiting for pipes");
            }

            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > CONNECTION_TIMEOUT)
            {
                logger_.error("Timeout waiting for pipes to be created");
                throw std::runtime_error("Timeout waiting for pipes");
            }

            std::this_thread::sleep_for(POLL_INTERVAL);
        }

        logger_.info("Pipes found, connecting (epoll)...");

        // Open read pipe with O_NONBLOCK
        start = std::chrono::steady_clock::now();
        while ((reader_fd_ = open(PIPE_TO_RECEIVER, O_RDONLY | O_NONBLOCK)) == -1)
        {
            if (errno == ENXIO)
            {
                auto elapsed = std::chrono::steady_clock::now() - start;
                if (elapsed > CONNECTION_TIMEOUT)
                {
                    logger_.error("Timeout connecting read pipe");
                    throw std::runtime_error("Timeout connecting read pipe");
                }

                if (signal_handler::should_exit())
                {
                    throw std::runtime_error("Interrupted while connecting");
                }

                std::this_thread::sleep_for(POLL_INTERVAL);
                continue;
            }

            logger_.error(std::string("Failed to open read pipe: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open read pipe");
        }
        logger_.debug("Read pipe opened (non-blocking)");

        // Add read fd to epoll for monitoring incoming data from producer
        add_to_epoll(reader_fd_, EPOLLIN);

        // Open write pipe with O_NONBLOCK
        start = std::chrono::steady_clock::now();
        while ((writer_fd_ = open(PIPE_TO_SENDER, O_WRONLY | O_NONBLOCK)) == -1)
        {
            if (errno == ENXIO)
            {
                auto elapsed = std::chrono::steady_clock::now() - start;
                if (elapsed > CONNECTION_TIMEOUT)
                {
                    close(reader_fd_);
                    reader_fd_ = -1;
                    logger_.error("Timeout connecting write pipe");
                    throw std::runtime_error("Timeout connecting write pipe");
                }

                if (signal_handler::should_exit())
                {
                    close(reader_fd_);
                    throw std::runtime_error("Interrupted while connecting");
                }

                std::this_thread::sleep_for(POLL_INTERVAL);
                continue;
            }

            close(reader_fd_);
            reader_fd_ = -1;
            logger_.error(std::string("Failed to open write pipe: ") + strerror(errno));
            throw std::system_error(errno, std::system_category(),
                                    "Failed to open write pipe");
        }
        logger_.debug("Write pipe opened (non-blocking)");

        // Add write fd to epoll for monitoring writability
        add_to_epoll(writer_fd_, EPOLLOUT);
    }

    bool process_as_producer()
    {
        struct epoll_event events[1];

        // Step 1: Send current counter to consumer
        {
            auto bytes_written = write(writer_fd_, &counter_, sizeof(counter_));

            if (bytes_written <= 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    logger_.debug("Write would block, will retry");
                    return false;
                }
                if (errno == EPIPE)
                {
                    logger_.warning("Consumer disconnected (broken pipe)");
                }
                else
                {
                    logger_.error(std::string("Write error: ") + strerror(errno));
                }
                return true;
            }

            {
                std::ostringstream oss;
                oss << "Sent counter: " << counter_;
                logger_.info(oss.str());
            }
        }

        // Step 2: Wait for consumer response
        while (true)
        {
            int nfds = epoll_wait(epoll_fd_, events, 1,
                                  static_cast<int>(EPOLL_TIMEOUT.count()));

            if (nfds == 0)
            {
                logger_.debug("Waiting for consumer response...");
                if (signal_handler::should_exit())
                {
                    return true;
                }
                continue;
            }

            if (nfds == -1)
            {
                if (errno == EINTR)
                {
                    if (signal_handler::should_exit())
                        return true;
                    continue;
                }
                logger_.error(std::string("Epoll wait error: ") + strerror(errno));
                return true;
            }

            if (events[0].data.fd == reader_fd_ && (events[0].events & EPOLLIN))
            {
                int received{0};
                auto bytes_read = read(reader_fd_, &received, sizeof(received));

                if (bytes_read > 0)
                {
                    {
                        std::ostringstream oss;
                        oss << "Received from consumer: " << received;
                        logger_.info(oss.str());
                    }

                    counter_ = received + 1;

                    if (counter_ >= MAX_COUNTER)
                    {
                        logger_.notice("Producer reached max counter");
                        return true;
                    }

                    // Got response, break out to send next counter
                    break;
                }
                else if (bytes_read == 0)
                {
                    logger_.warning("Consumer closed connection (EOF)");
                    return true;
                }
                else if (bytes_read == -1 && errno != EAGAIN)
                {
                    logger_.error(std::string("Read error: ") + strerror(errno));
                    return true;
                }
            }

            if (events[0].events & (EPOLLHUP | EPOLLERR))
            {
                logger_.warning("Consumer pipe error or hangup");
                return true;
            }
        }

        std::this_thread::sleep_for(PROCESS_INTERVAL);
        return false;
    }

    bool process_as_consumer()
    {
        struct epoll_event events[1];

        // Step 1: Wait for producer data
        while (true)
        {
            int nfds = epoll_wait(epoll_fd_, events, 1,
                                  static_cast<int>(EPOLL_TIMEOUT.count()));

            if (nfds == 0)
            {
                logger_.debug("Waiting for producer data...");
                if (signal_handler::should_exit())
                {
                    return true;
                }
                continue;
            }

            if (nfds == -1)
            {
                if (errno == EINTR)
                {
                    if (signal_handler::should_exit())
                        return true;
                    continue;
                }
                logger_.error(std::string("Epoll wait error: ") + strerror(errno));
                return true;
            }

            if (events[0].data.fd == reader_fd_ && (events[0].events & EPOLLIN))
            {
                int received{0};
                auto bytes_read = read(reader_fd_, &received, sizeof(received));

                if (bytes_read > 0)
                {
                    {
                        std::ostringstream oss;
                        oss << "Received from producer: " << received;
                        logger_.info(oss.str());
                    }

                    counter_ = received + 1;

                    // Step 2: Send back to producer
                    auto bytes_written = write(writer_fd_, &counter_, sizeof(counter_));

                    if (bytes_written > 0)
                    {
                        {
                            std::ostringstream oss;
                            oss << "Sent back: " << counter_;
                            logger_.info(oss.str());
                        }

                        if (counter_ >= MAX_COUNTER)
                        {
                            logger_.notice("Consumer reached max counter");
                            return true;
                        }

                        // Successfully received and sent, break to wait for next
                        break;
                    }
                    else if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        logger_.debug("Write would block, will retry");
                        // Wait for epoll to signal write readiness
                        // For now, just loop and retry
                        continue;
                    }
                    else if (errno == EPIPE)
                    {
                        logger_.warning("Producer disconnected");
                        return true;
                    }
                    else
                    {
                        logger_.error(std::string("Write error: ") + strerror(errno));
                        return true;
                    }
                }
                else if (bytes_read == 0)
                {
                    logger_.warning("Producer closed connection (EOF)");
                    return true;
                }
                else if (bytes_read == -1 && errno != EAGAIN)
                {
                    logger_.error(std::string("Read error: ") + strerror(errno));
                    return true;
                }
            }

            if (events[0].events & (EPOLLHUP | EPOLLERR))
            {
                logger_.warning("Producer pipe error or hangup");
                return true;
            }
        }

        return false;
    }
};