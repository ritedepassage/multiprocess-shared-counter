#include <iostream>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <csignal>
#include <memory>

#include "blocking-pipe-manager.hh"
#include "nonblocking-pipe-manager.hh"

int main(int argc, char **argv)
{
    bool is_intiator = true;

    if (argc >= 2)
    {
        if (strcmp(argv[1], "-t") == 0)
        {
            if (strcmp(argv[2], "rec") == 0)
            {
                printf("process is receiver\n");
                is_intiator = false;
            }
            else if (strcmp(argv[2], "init") != 0)
                printf("unknown process type (%s) - considering the process as initiator\n", argv[2]);
        }
    }

    try
    {
        std::unique_ptr<ipc_manager<nonblocking_pipe_manager>> non_ipc_mgr;

        if (is_intiator)
        {
            non_ipc_mgr = std::make_unique<nonblocking_pipe_manager>(true);
        }
        else
        {
            non_ipc_mgr = std::make_unique<nonblocking_pipe_manager>(false);
        }

        non_ipc_mgr->run();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Fatal error: {}" << e.what() << std::endl;
        return -1;
    }

    return 0;
}