# CSM218 Distributed Matrix Multiplication Submission

**Name:** Batsirai Manyengura  
**Student Number:** N02220241t  
**GitHub Username:** j-batsie  
**Repository Link:** [https://github.com/CSM218/ipc-rpc-parallel-computing-j-batsie.git](https://github.com/CSM218/ipc-rpc-parallel-computing-j-batsie.git)

## Project Overview
This project implements a distributed matrix multiplication system for CSM218 Lab 1, using a custom binary protocol and Java concurrency primitives. The system is designed for fault tolerance, parallelism, and autograder compliance.

## Key Features
- Custom binary protocol (see `src/main/java/pdc/Message.java`)
- Master/Worker architecture with heartbeat and recovery
- Thread pool and concurrent collections for parallel task execution
- Resource management and clean shutdown for testability

## How to Build and Test
- Build: `./gradlew build --no-daemon`
- Run tests: `./gradlew test --no-daemon`
- Run autograder: `cd autograder && python3 grade.py`

## Notes
- All background threads are properly shut down in tests to avoid hangs.
- Please see the code and README for further details.
