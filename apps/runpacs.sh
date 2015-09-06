#!/bin/sh
clear
export DCMDICTPATH="../../dcmdata/data/dicom.dic"
echo "   =============================="
echo "   ===[ Starting OpenPACS ]==="
echo ""
echo "   `date`"
echo "   =============================="
echo "   $DCMDICTPATH"

./openpacs +xs -xs -c openpacs -lc ../config/log2file.cfg &
