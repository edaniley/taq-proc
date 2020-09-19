
add_executable( tick-calc
    tick-calc.cpp
    tick-conn.cpp
    tick-data.cpp
    tick-duration.cpp
    tick-exec.cpp
    tick-json.cpp
    tick-log.cpp
    tick-net.cpp
    tick-func.cpp
    tick-func-nbbo.cpp
    tick-func-rod.cpp
    tick-func-vwap.cpp
    tick-func-vwap-calc.cpp
)

TARGET_LINK_LIBRARIES( tick-calc
    pthread
)