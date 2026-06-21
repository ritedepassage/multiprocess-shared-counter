#include <iostream>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <csignal>

#include "blocking-pipe-manager.hh"

volatile bool should_exit = false;

void sginal_handler(int sig)
{
    printf("signal %d received\n", sig);

    should_exit = true;
}

int main(int argc, char **argv)
{
    int write_fd;
    int read_fd;

    ipc_manager<blocking_pipe_manager> *ipc_mgr;

    int counter = 0;
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

    signal(SIGINT, sginal_handler);

    if (is_intiator)
    {
        blocking_pipe_manager blocking_pipe{true};

        ipc_mgr = &blocking_pipe;

        ipc_mgr->initialize();

        while (!should_exit)
            should_exit = ipc_mgr->process_messages();

        ipc_mgr->cleanup();
    }
    else
    {
        blocking_pipe_manager blocking_pipe{false};

        ipc_mgr = &blocking_pipe;

        ipc_mgr->initialize();

        while (!should_exit)
            should_exit = ipc_mgr->process_messages();

        ipc_mgr->cleanup();
    }

    return 0;
}