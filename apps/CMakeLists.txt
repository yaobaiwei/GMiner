#Copyright 2018 Husky Data Lab, CUHK
#Authors: Hongzhi Chen, Miao Liu


include_directories(${PROJECT_SOURCE_DIR} ${GMINER_EXTERNAL_INCLUDES})
link_directories(${GMINER_EXTERNAL_LIBRARIES})

#### APPS ####

# Triangle Counting
add_executable(tc trianglecount.cpp)
target_link_libraries(tc gminer-util ${COMMON_LINK_LIBS})

# Max Clique
add_executable(mc maxclique.cpp)
target_link_libraries(mc gminer-util ${COMMON_LINK_LIBS})

# Graph Match
add_executable(gm graphmatch.cpp)
target_link_libraries(gm gminer-util ${COMMON_LINK_LIBS})

# Community Detection
add_executable(cd community.cpp)
target_link_libraries(cd gminer-util ${COMMON_LINK_LIBS})

# Focus CO
add_executable(fco focusCO.cpp)
target_link_libraries(fco gminer-util ${COMMON_LINK_LIBS})
