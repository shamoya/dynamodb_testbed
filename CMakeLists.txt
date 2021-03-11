cmake_minimum_required(VERSION 3.15)
project(xcachedb_testbed)

set(CMAKE_CXX_STANDARD 11)
find_package(AWSSDK REQUIRED COMPONENTS dynamodb)
add_executable(put_items put_items.cpp)
target_link_libraries(put_items ${AWSSDK_LINK_LIBRARIES})
add_executable(get_items get_items.cpp)
target_link_libraries(get_items ${AWSSDK_LINK_LIBRARIES})
