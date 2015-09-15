#!/bin/sh
export DCMDICTPATH="../dcmtk-3.6.1_20150629/dcmdata/data/dicom.dic"
echo "   =============================="
echo "   ===[ Starting OpenPACS ]==="
echo ""
echo "   `date`"
echo "   =============================="
echo "   $DCMDICTPATH"
echo "   To start in port 104 this must be run as root."

./openpacssrv +xs -xs -c ../config/openpacssrv.cfg -lc ../config/log2file.cfg &
