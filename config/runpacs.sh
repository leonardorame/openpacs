#!/bin/sh
clear
export DCMDICTPATH="../../dcmdata/data/dicom.dic"
echo "   =============================="
echo "   ===[ Starting G-Pacs core ]==="
echo ""
echo "   `date`"
echo "   =============================="
echo "   $DCMDICTPATH"

#./dcmqrscp +xs -xs -c dcmqrscp.cfg -lc log2file.cfg &
./dcmqrscp +xs -xs -dhl -v -c dcmqrscp.cfg 
