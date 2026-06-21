#include <iostream>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <csignal>
#include <memory>

#include "blocking-pipe-manager.hh"

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
        std::unique_ptr<ipc_manager<blocking_pipe_manager>> ipc_mgr;

        if (is_intiator)
        {
            ipc_mgr = std::make_unique<blocking_pipe_manager>(true);
        }
        else
        {
            ipc_mgr = std::make_unique<blocking_pipe_manager>(false);
        }

        ipc_mgr->run();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Fatal error: {}" << e.what() << std::endl;
        return -1;
    }

    return 0;
}