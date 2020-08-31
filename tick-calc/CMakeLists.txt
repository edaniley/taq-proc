
add_executable( tick-calc
    tick-calc.cpp
    tick-data.cpp
    tick-exec.cpp
    tick-net.cpp
    tick-log.cpp
    tick-func-quote.cpp
    tick-func-rod.cpp
    tick-func-vwap.cpp
)

TARGET_LINK_LIBRARIES( tick-calc
    pthread
)