cmake_minimum_required(VERSION 3.0)
#variables
set(dcmtk_root ../dcmtk-3.6.1_20150629)
set(DCMTK_LIB ${dcmtk_root}/lib)
#set(OPENJPEG /usr/local/lib/openjpeg-2.1)
#include openjpeg
#include(${OPENJPEG}/OpenJPEGConfig.cmake)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# declare include directories which hold for all subdirectories
INCLUDE_DIRECTORIES(../include ${osconfig_SOURCE_DIR}/include ${ofstd_SOURCE_DIR}/include ${oflog_SOURCE_DIR}/include ${oflog_SOURCE_DIR}/include ${dcmdata_SOURCE_DIR}/include ${dcmnet_SOURCE_DIR}/include ${ZLIB_INCDIR} /usr/include/postgresql /usr/local/include/openjpeg-2.1 /usr/local/include/fmjpeg2k )

# declare executables
ADD_EXECUTABLE(openpacssrv openpacs.cc)
# make sure executables are linked to the corresponding libraries
find_library(DCMTK_OFLOG oflog HINTS ${DCMTK_LIB})
find_library(DCMTK_OFSTD ofstd HINTS ${DCMTK_LIB})
find_library(DCMTK_DCMNET dcmnet HINTS ${DCMTK_LIB})
find_library(DCMTK_DCMDATA dcmdata HINTS ${DCMTK_LIB})
find_library(DCMTK_DCMIMGLE dcmimgle HINTS ${DCMTK_LIB})
find_library(DCMTK_DCMIMAGE dcmimage HINTS ${DCMTK_LIB})
find_library(DCMTK_DCMJPEG dcmjpeg HINTS ${DCMTK_LIB})
find_library(DCMTK_IJG8 ijg8 HINTS ${DCMTK_LIB})
find_library(DCMTK_IJG12 ijg12 HINTS ${DCMTK_LIB})
find_library(DCMTK_IJG16 ijg16 HINTS ${DCMTK_LIB})
TARGET_LINK_LIBRARIES(openpacssrv 
  openpacs 
  ${DCMTK_OFLOG} 
  ${DCMTK_DCMJPEG} 
  ${DCMTK_IJG8} 
  ${DCMTK_IJG12} 
  ${DCMTK_IJG16} 
  ${DCMTK_DCMIMAGE} 
  ${DCMTK_DCMIMGLE} 
  ${DCMTK_DCMNET} 
  ${DCMTK_DCMDATA} 
  ${DCMTK_OFSTD} 
  fmjpeg2k openjp2 pq dl pthread z)



