cmake_minimum_required(VERSION 3.17)
project(SQLClone C)

set(CMAKE_C_STANDARD 11)

add_library(SQLClone library.c library.h)

add_executable(SQLCloneExp main.c)