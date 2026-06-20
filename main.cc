#include <iostream>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <csignal>

volatile bool exit_loop = false;

void sginal_handler(int sig)
{
    printf("signal %d received\n", sig);

    exit_loop = true;
}

int main(int argc, char **argv)
{
    int write_fd;
    int read_fd;

    const char *pipe_to_receiver = "/tmp/counter_to_receiver";
    const char *pipe_to_initiator = "/tmp/counter_to_initiator";

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
        printf("Initiator starting (PID: %d)\n", getpid());

        // Create both pipes
        unlink(pipe_to_receiver);
        unlink(pipe_to_initiator);

        mkfifo(pipe_to_receiver, 0666);
        mkfifo(pipe_to_initiator, 0666);

        printf("Waiting for receiver...\n");

        // Open write pipe (to receiver)
        write_fd = open(pipe_to_receiver, O_WRONLY);
        if (write_fd < 0)
        {
            perror("open write pipe");
            return -1;
        }

        // Open read pipe (from receiver)
        read_fd = open(pipe_to_initiator, O_RDONLY);
        if (read_fd < 0)
        {
            perror("open read pipe");
            close(write_fd);
            return -1;
        }

        printf("Receiver connected! Starting counter exchange.\n");

        // Send initial counter
        ssize_t res = write(write_fd, &counter, sizeof(counter));
        printf("Initiator: Sent counter = %d\n", counter);

        // Exchange loop
        while (!exit_loop)
        {
            // Read updated counter from receiver
            ssize_t bytes = read(read_fd, &counter, sizeof(counter));
            if (bytes > 0)
            {
                printf("Initiator: Received counter = %d from receiver\n", counter);
            }
            else if (bytes == 0)
            {
                printf("Receiver disconnected\n");
                break;
            }

            // Increment and send back
            counter++;
            sleep(1); // Simulate processing

            res = write(write_fd, &counter, sizeof(counter));
            if (res > 0)
            {
                printf("Initiator: Sent counter = %d to receiver\n", counter);
            }
            else
            {
                printf("Write failed\n");
                break;
            }

            if (counter >= 10)
                break;
        }

        close(write_fd);
        close(read_fd);
        unlink(pipe_to_receiver);
        unlink(pipe_to_initiator);
    }
    else
    {
        printf("Receiver starting (PID: %d)\n", getpid());

        // Wait for pipes
        while (access(pipe_to_receiver, F_OK) == -1 && !exit_loop)
        {
            usleep(100000);
        }
        while (access(pipe_to_initiator, F_OK) == -1 && !exit_loop)
        {
            usleep(100000);
        }

        // Open read pipe (from initiator) FIRST
        read_fd = open(pipe_to_receiver, O_RDONLY);
        if (read_fd < 0)
        {
            perror("open read pipe");
            return -1;
        }

        // Then open write pipe (to initiator)
        write_fd = open(pipe_to_initiator, O_WRONLY);
        if (write_fd < 0)
        {
            perror("open write pipe");
            close(read_fd);
            return -1;
        }

        printf("Connected to initiator!\n");

        // Exchange loop
        while (!exit_loop)
        {
            // Read counter from initiator
            ssize_t bytes = read(read_fd, &counter, sizeof(counter));
            if (bytes > 0)
            {
                printf("Receiver: Received counter = %d from initiator\n", counter);
            }
            else if (bytes == 0)
            {
                printf("Initiator disconnected\n");
                break;
            }

            if (counter >= 10)
                break;

            // Increment and send back
            counter++;
            sleep(1);  // Simulate processing

            ssize_t res = write(write_fd, &counter, sizeof(counter));
            if (res > 0)
            {
                printf("Receiver: Sent counter = %d to initiator\n", counter);
            }
            else
            {
                printf("Write failed\n");
                break;
            }
        }

        close(read_fd);
        close(write_fd);
    }

    return 0;
}