#!/bin/sh
clear
export DCMDICTPATH="../dcmtk-3.6.1_20150629/dcmdata/data/dicom.dic"
echo "   =============================="
echo "   ===[ Starting OpenPACS ]==="
echo ""
echo "   `date`"
echo "   =============================="
echo "   $DCMDICTPATH"

./openpacs +xs -xs -c openpacs -lc ../config/log2file.cfg &
