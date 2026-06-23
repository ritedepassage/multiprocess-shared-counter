*Use the following commands in order to build*  
```
   cd multiprocess-shared-counter
   mkdir build
   cd build
   cmake ..
   make
```

*Usage: multiprocess-shared-counter [-t <type>] [-m <mode>]*  

```
Options:
  -t <type>    Process type (optional, default: init)
               Values:
                 init  - Producer/Initiator process
                 rec   - Receiver/Consumer process

  -m <mode>    IPC mechanism (optional, default: nonblocking)
               Values:
                 blocking     - Blocking named pipes
                 nonblocking  - Non-blocking pipes with epoll
```

*Examples*  
```
# Terminal 1 (must run first)
./mpsc -t init

# Terminal 2
./mpsc -t rec
```

*Monitoring with syslog*
```
journalctl -f -t counter_ipc
```