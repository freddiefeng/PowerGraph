project(example)
add_graphlab_executable(edge_update edge_update.cpp)
set(CMAKE_CXX_FLAGS                "-Wall -std=c++11")