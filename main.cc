#include <iostream>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <csignal>
#include <memory>

#include "blocking-pipe-manager.hh"
#include "nonblocking-pipe-manager.hh"

void print_usage(const char *prog_name)
{
    std::cout << "Usage: " << prog_name << " -t <init|rec> [-m <blocking|nonblocking>]\n";
    std::cout << "Options:\n";
    std::cout << "  -t <type>    Process type: 'init' (producer) or 'rec' (consumer)\n";
    std::cout << "  -m <mode>    IPC mode: 'blocking' or 'nonblocking' (default: blocking)\n";
}

int main(int argc, char **argv)
{
    bool is_initiator = true;
    bool is_nonblocking = true;
    int opt;

    while ((opt = getopt(argc, argv, "t:m")) != -1)
    {
        switch (opt)
        {
        case 't':
        {
            if (optarg)
            {
                if (std::strcmp(optarg, "rec") == 0)
                {
                    is_initiator = false;
                }
                else if (std::strcmp(optarg, "init") == 0)
                {
                    is_initiator = true;
                }
                else
                {
                    std::cerr << "Invalid type: " << optarg << ". Use 'init' or 'rec'\n";
                    print_usage(argv[0]);
                    return 1;
                }
            }
        }
        break;
        case 'm':
        {
            if (optarg)
            {
                if (std::strcmp(optarg, "nonblocking") == 0)
                {
                    is_nonblocking = true;
                }
                else if (std::strcmp(optarg, "blocking") == 0)
                {
                    is_nonblocking = false;
                }
                else
                {
                    std::cerr << "Invalid mode: " << optarg << ". Use 'blocking' or 'nonblocking'\n";
                    print_usage(argv[0]);
                    return 1;
                }
            }
        }
        break;
        default:
        {
            print_usage(argv[0]);
            return 1;
        }
        }
    }

    try
    {

        if (is_nonblocking)
        {
            std::unique_ptr<ipc_manager<nonblocking_pipe_manager>> ipc_mgr;
            ipc_mgr = std::make_unique<nonblocking_pipe_manager>(is_initiator);
            ipc_mgr->run();
        }
        else
        {
            std::unique_ptr<ipc_manager<blocking_pipe_manager>> ipc_mgr;
            ipc_mgr = std::make_unique<blocking_pipe_manager>(is_initiator);
            ipc_mgr->run();
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}