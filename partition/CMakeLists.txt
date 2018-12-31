#Copyright 2018 Husky Data Lab, CUHK
#Authors: Hongzhi Chen, Miao Liu

include_directories(${PROJECT_SOURCE_DIR} ${GMINER_EXTERNAL_INCLUDES})
link_directories(${GMINER_EXTERNAL_LIBRARIES})

file(GLOB partition-src-files
    BGlobal.cpp
    )

add_library(gminer-partition ${partition-src-files})
target_link_libraries(gminer-partition ${COMMON_LINK_LIBS})
target_include_directories (gminer-partition PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(partition partition.cpp)
target_link_libraries(partition gminer-partition gminer-util ${COMMON_LINK_LIBS})
