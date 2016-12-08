cmake_minimum_required(VERSION 3.6)
project(coursera)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
    assignment1/mp1/Application.cpp
    assignment1/mp1/Application.h
    assignment1/mp1/EmulNet.cpp
    assignment1/mp1/EmulNet.h
    assignment1/mp1/Log.cpp
    assignment1/mp1/Log.h
    assignment1/mp1/Member.cpp
    assignment1/mp1/Member.h
    assignment1/mp1/MP1Node.cpp
    assignment1/mp1/MP1Node.h
    assignment1/mp1/Params.cpp
    assignment1/mp1/Params.h
    assignment1/mp1/Queue.h
    assignment1/mp1/stdincludes.h)

add_executable(coursera ${SOURCE_FILES})