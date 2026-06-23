#ifndef _BLOCKING_PIPE_MANAGER_H_
#define _BLOCKING_PIPE_MANAGER_H_

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>
#include <thread>
#include <string>
#include <cstring>

#include "ipc-manager.hh"

class blocking_pipe_manager : public ipc_manager<blocking_pipe_manager>
{
public:
    explicit blocking_pipe_manager(bool is_producer) : ipc_manager{"blocking_pipe", is_producer}
    {
    }

    void initialize()
    {
        if (is_producer_)
        {
            logger_.info("Initializing as PRODUCER");
            setup_as_producer();
        }
        else
        {
            logger_.info("Initializing as CONSUMER");
            setup_as_consumer();
        }
        logger_.info(std::string("Pipes connected. Writer fd = ") +
                     std::to_string(writer_fd_) +
                     std::string("Reader fd = ") +
                     std::to_string(reader_fd_));

        is_initialized_ = true;
    }

    bool process_messages()
    {
        if (!is_initialized_)
        {
            logger_.error("Process messages called before initialization");
            return true;
        }

        if (is_producer_)
        {
            auto bytes_written = write(writer_fd_, (const void *)&counter_, sizeof(counter_));
            if (bytes_written <= 0)
            {
                std::cout << "failed to write on producer " << counter_ << std::endl;
                return true;
            }

            int received{0};
            auto bytes_read = read(reader_fd_, (void *)&received, sizeof(received));
            if (bytes_read)
            {
                std::cout << "producer received " << received << std::endl;
                counter_ = received + 1;

                if (counter_ >= MAX_COUNTER)
                {
                    std::cout << "producer process finished with counter " << counter_ << std::endl;
                    return true;
                }
            }
            else if (bytes_read == 0)
            {
                std::cout << "Consumer closed connection" << std::endl;
                return true;
            }
            else
            {
                std::cout << std::string("Read error: ") + std::strerror(errno);
                return true;
            }
        }
        else
        {
            int received{0};
            auto bytes_read = read(reader_fd_, (void *)&received, sizeof(received));
            if (bytes_read)
            {
                std::cout << "consumer received " << received << std::endl;
                counter_ = received + 1;

                auto bytes_written = write(writer_fd_, (const void *)&counter_, sizeof(counter_));
                if (bytes_written <= 0)
                {
                    std::cout << "failed to write on consumer " << counter_ << std::endl;
                    return true;
                }

                if (counter_ >= MAX_COUNTER)
                {
                    std::cout << "consumer process finished with counter " << counter_ << std::endl;
                    return true;
                }
            }
            else if (bytes_read == 0)
            {
                std::cout << "Producer closed connection" << std::endl;
                return true;
            }
            else
            {
                std::cout << std::string("Read error: ") + std::strerror(errno);
                return true;
            }
        }
        std::this_thread::sleep_for(PROCESS_INTERVAL);
        return false;
    }

    void cleanup() noexcept
    {
        if (!is_initialized_)
        {
            return; // Nothing to cleanup
        }

        logger_.info("Cleaning up file descriptors");
        if (writer_fd_ >= 0)
        {
            close(writer_fd_);
            writer_fd_ = -1;
        }
        if (reader_fd_ >= 0)
        {
            close(reader_fd_);
            reader_fd_ = -1;
        }
        if (is_producer_)
        {
            unlink(PIPE_TO_RECEIVER);
            unlink(PIPE_TO_SENDER);
            logger_.debug("Removed pipe files");
        }

        is_initialized_ = false;
    }

private:
    void setup_as_producer()
    {
        unlink(PIPE_TO_RECEIVER);
        unlink(PIPE_TO_SENDER);

        if (mkfifo(PIPE_TO_RECEIVER, 0666) == -1)
        {
            throw std::system_error(errno, std::system_category(), "Failed to create receiver pipe");
        }
        if (mkfifo(PIPE_TO_SENDER, 0666) == -1)
        {
            unlink(PIPE_TO_RECEIVER);
            throw std::system_error(errno, std::system_category(), "Failed to create intiator pipe");
        }

        writer_fd_ = open(PIPE_TO_RECEIVER, O_WRONLY);
        if (writer_fd_ == -1)
        {
            throw std::system_error(errno, std::system_category(), "Failed to open write pipe");
        }

        reader_fd_ = open(PIPE_TO_SENDER, O_RDONLY);
        if (reader_fd_ == -1)
        {
            close(writer_fd_);
            throw std::system_error(errno, std::system_category(), "Failed to open read pipe");
        }
    }

    void setup_as_consumer()
    {
        auto start = std::chrono::steady_clock::now();

        while ((access(PIPE_TO_RECEIVER, F_OK) == -1 ||
                access(PIPE_TO_SENDER, F_OK) == -1))
        {
            if (signal_handler::should_exit())
            {
                throw std::runtime_error("Interrupted while waiting for pipes");
            }

            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > CONNECTION_TIMEOUT)
            {
                auto timeout_seconds = std::chrono::duration_cast<std::chrono::seconds>(CONNECTION_TIMEOUT).count();
                throw std::runtime_error(std::string("Timeout waiting for pipes after ") + std::to_string(timeout_seconds) + std::string(" seconds"));
            }

            std::this_thread::sleep_for(POLL_INTERVAL);
        }

        reader_fd_ = open(PIPE_TO_RECEIVER, O_RDONLY);
        if (reader_fd_ == -1)
        {
            throw std::system_error(errno, std::system_category(), "Failed to open read pipe");
        }

        writer_fd_ = open(PIPE_TO_SENDER, O_WRONLY);
        if (writer_fd_ == -1)
        {
            close(reader_fd_);
            throw std::system_error(errno, std::system_category(), "Failed to open write pipe");
        }
    }
};

#endif