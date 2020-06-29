#Copyright 2018 Husky Data Lab, CUHK
#Authors: Hongzhi Chen, Miao Liu


include_directories(${PROJECT_SOURCE_DIR} ${GMINER_EXTERNAL_INCLUDES})
link_directories(${GMINER_EXTERNAL_LIBRARIES})

add_library(iniparser-objs OBJECT
    dictionary.c
    iniparser.c
    )
