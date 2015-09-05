cmake_minimum_required(VERSION 3.0)
# declare project
PROJECT(openpacs)

set(dcmtk_root dcmtk-3.6.1_20150629)
set(osconfig_SOURCE_DIR ${dcmtk_root}/config)
set(ofstd_SOURCE_DIR ${dcmtk_root}/ofstd)
set(oflog_SOURCE_DIR ${dcmtk_root}/oflog)
set(dcmnet_SOURCE_DIR ${dcmtk_root}/dcmnet)
set(dcmdata_SOURCE_DIR ${dcmtk_root}/dcmdata)
set(dcmjpeg_SOURCE_DIR ${dcmtk_root}/dcmjpeg)
set(dcmimgle_SOURCE_DIR ${dcmtk_root}/dcmimgle)
set(dcmimage_SOURCE_DIR ${dcmtk_root}/dcmimage)

INCLUDE_DIRECTORIES(
  fmjpeg2koj 
  include 
  ${osconfig_SOURCE_DIR}/include 
  ${ofstd_SOURCE_DIR}/include 
  ${oflog_SOURCE_DIR}/include 
  ${dcmdata_SOURCE_DIR}/include 
  ${dcmnet_SOURCE_DIR}/include 
  ${dcmjpeg_SOURCE_DIR}/include 
  ${dcmimgle_SOURCE_DIR}/include 
  ${dcmimage_SOURCE_DIR}/include 
  ${ZLIB_INCDIR}
  /usr/include/postgresql)

# recurse into subdirectories
FOREACH(SUBDIR libsrc include/dcmtk/dcmqrdb apps)
  ADD_SUBDIRECTORY(${SUBDIR})
ENDFOREACH(SUBDIR)

