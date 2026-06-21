#ifndef _BLOCKING_PIPE_MANAGER_H_
#define _BLOCKING_PIPE_MANAGER_H_

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>
#include <thread>

#include "ipc-manager.hh"

class blocking_pipe_manager : public ipc_manager<blocking_pipe_manager>
{
private:
    int writer_fd_{-1};
    int reader_fd_{-1};
    bool is_producer_;

    static constexpr const char *PIPE_TO_RECEIVER = "/tmp/counter_to_receiver";
    static constexpr const char *PIPE_TO_SENDER = "/tmp/counter_to_sender";

public:
    explicit blocking_pipe_manager(bool is_producer) : ipc_manager{"blocking_pipe"}, is_producer_{is_producer}
    {
    }

    void initialize()
    {
        if (is_producer_)
        {
            std::cout << "blocking producer initialize" << std::endl;
            setup_as_producer();
        }
        else
        {
            std::cout << "blocking consumer initialize" << std::endl;
            setup_as_consumer();
        }
    }

    bool process_messages()
    {
        std::cout << "blocking process messages" << std::endl;
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
                counter_ = received;
                ++counter_;
                if (counter_ >= 10)
                {
                    std::cout << "producer process finished with counter " << counter_ << std::endl;
                    return true;
                }
            }
            else
            {
                std::cout << "peer disconnected current counter " << counter_ << std::endl;
                return true;
            }
        }
        else
        {
            int received{0};
            auto bytes_read = read(reader_fd_, (void *)&received, sizeof(received));
            if (bytes_read)
            {
                counter_ = received;
                ++counter_;

                auto bytes_written = write(writer_fd_, (const void *)&counter_, sizeof(counter_));
                if (bytes_written <= 0)
                {
                    std::cout << "failed to write on consumer " << counter_ << std::endl;
                    return true;
                }

                if (counter_ >= 10)
                {
                    std::cout << "consumer process finished with counter " << counter_ << std::endl;
                    return true;
                }
            }
            else
            {
                std::cout << "peer disconnected current counter " << counter_ << std::endl;
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        return false;
    }

    void cleanup()
    {
        std::cout << "blocking cleanup" << std::endl;
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
        }
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

        reader_fd_ = open(PIPE_TO_SENDER, O_RDONLY);
    }

    void setup_as_consumer()
    {

        while ((access(PIPE_TO_RECEIVER, F_OK) == -1 ||
                access(PIPE_TO_SENDER, F_OK) == -1))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        reader_fd_ = open(PIPE_TO_RECEIVER, O_RDONLY);

        writer_fd_ = open(PIPE_TO_SENDER, O_WRONLY);
    }
};

#endif