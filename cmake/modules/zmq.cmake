### ZeroMQ ###

IF (ZMQ_INCLUDE_DIR)
    SET (ZMQ_FIND_QUIETLY TRUE)
ENDIF (ZMQ_INCLUDE_DIR)

message(${ZMQ_ROOT})

# find includes
FIND_PATH (ZMQ_INCLUDE_DIR
    NAMES zmq.h
    HINTS
    ${ZMQ_ROOT}/include
)

# find lib
SET(ZMQ_NAME zmq)

FIND_LIBRARY(ZMQ_LIBRARIES
    NAMES ${ZMQ_NAME}
    PATHS ${ZMQ_ROOT}/lib
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("ZMQ" DEFAULT_MSG ZMQ_INCLUDE_DIR ZMQ_LIBRARIES)

mark_as_advanced (ZMQ_INCLUDE_DIR ZMQ_LIBRARIES)
