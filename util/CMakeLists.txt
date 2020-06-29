#Copyright 2018 Husky Data Lab, CUHK
#Authors: Hongzhi Chen, Miao Liu


include_directories(${PROJECT_SOURCE_DIR} ${GMINER_EXTERNAL_INCLUDES})
link_directories(${GMINER_EXTERNAL_LIBRARIES})

add_subdirectory(iniparser)
 
add_library(util-objs OBJECT
    global.cpp
    hdfs_core.cpp
    minhash.cpp
    threadpool.cpp
    timer.cpp
    )

add_library(gminer-util STATIC $<TARGET_OBJECTS:util-objs> $<TARGET_OBJECTS:iniparser-objs>)

target_link_libraries(gminer-util ${COMMON_LINK_LIBS})
target_include_directories (gminer-util PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
