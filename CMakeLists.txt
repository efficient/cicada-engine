CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(MICA)


SET(CMAKE_CXX_COMPILER g++-5)
SET(CMAKE_C_LINK_EXECUTABLE g++-5)



OPTION(DEBUG "Enable debugging" OFF)
OPTION(LTO "Use link time optimization" OFF)

# OPTION(AVX2 "Enable AVX2" ON)



INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/src)
SET(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build)
SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build)
SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build)



ADD_DEFINITIONS(-std=c++14)

ADD_DEFINITIONS(-Wall -Wextra)
ADD_DEFINITIONS(-Wzero-as-null-pointer-constant)
ADD_DEFINITIONS(-Wsign-conversion)
ADD_DEFINITIONS(-Wconversion)
ADD_DEFINITIONS(-Winline)

ADD_DEFINITIONS(-Wno-unused-function)

ADD_DEFINITIONS(-march=native)
ADD_DEFINITIONS(-pthread)

ADD_DEFINITIONS(-g)



IF(DEBUG)

  MESSAGE(WARNING "Debugging is ENABLED (to disable, run `cmake .. -DDEBUG=OFF`). Performance will be low.")
  ADD_DEFINITIONS(-Og)
  # ADD_DEFINITIONS(-O0)

ELSE(DEBUG)

  MESSAGE(STATUS "Debugging is disabled (to enable, run `cmake .. -DDEBUG=ON`)")
  ADD_DEFINITIONS(-DNDEBUG)
  ADD_DEFINITIONS(-O3)
  # ADD_DEFINITIONS(--param inline-unit-growth=200 --param large-function-growth=500)

ENDIF(DEBUG)


IF(LTO)

  MESSAGE(STATUS "Link time optimization is enabled (to disable, run `cmake .. -DLTO=OFF`)")
  ADD_DEFINITIONS(-flto)
  SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")

ELSE(LTO)

  MESSAGE(WARNING "Link time optimization is DISABLED (to enable, run `cmake .. -DLTO=ON`). Performance will be low.")

ENDIF(LTO)


# IF(AVX2)

#   MESSAGE(STATUS "AVX2 is enabled (to disable, run `cmake .. -DAVX2=OFF`)")
#   ADD_DEFINITIONS(-mavx2)
#   SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -mavx2")

# ELSE(LTO)

#   MESSAGE(WARNING "AVX2 is DISABLED (to enable, run `cmake .. -DAVX2=ON`). Performance will be low.")

# ENDIF(AVX2)



SET(SOURCES ${SOURCES} src/mica/alloc/hugetlbfs_shm.cc)
SET(SOURCES ${SOURCES} src/mica/transaction/timestamp.cc)
SET(SOURCES ${SOURCES} src/mica/util/config.cc)
SET(SOURCES ${SOURCES} src/mica/util/cityhash/city_mod.cc)
SET(SOURCES ${SOURCES} src/mica/util/siphash/siphash24.c)
SET_SOURCE_FILES_PROPERTIES(src/mica/util/siphash/siphash24.c PROPERTIES LANGUAGE CXX)
SET(SOURCES ${SOURCES} src/mica/util/lcore.cc)
SET(SOURCES ${SOURCES} src/mica/util/stopwatch.cc)
SET(SOURCES ${SOURCES} src/mica/util/zipf.cc)

SET(LIBRARIES ${LIBRARIES} rt numa pthread)



IF(LTO)

  ADD_EXECUTABLE(test_atomics src/mica/test/test_atomics.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_atomics ${LIBRARIES})

  ADD_EXECUTABLE(test_tsc_sync src/mica/test/test_tsc_sync.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_tsc_sync ${LIBRARIES})

  ADD_EXECUTABLE(test_tx src/mica/test/test_tx.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_tx ${LIBRARIES})

  ADD_EXECUTABLE(test_tx_index src/mica/test/test_tx_index.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_tx_index ${LIBRARIES})

ELSE(LTO)

  ADD_LIBRARY(common ${SOURCES})

  ADD_EXECUTABLE(test_atomics src/mica/test/test_atomics.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_atomics ${LIBRARIES})

  ADD_EXECUTABLE(test_tsc_sync src/mica/test/test_tsc_sync.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_tsc_sync ${LIBRARIES})

  ADD_EXECUTABLE(test_tx src/mica/test/test_tx.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_tx ${LIBRARIES})

  ADD_EXECUTABLE(test_tx_index src/mica/test/test_tx_index.cc ${SOURCES})
  TARGET_LINK_LIBRARIES(test_tx_index ${LIBRARIES})

ENDIF(LTO)
