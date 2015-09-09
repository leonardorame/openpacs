/*
 *
 *  Copyright (C) 1993-2009, OFFIS
 *
 *  This software and supporting documentation were developed by
 *
 *    Kuratorium OFFIS e.V.
 *    Healthcare Information and Communication Systems
 *    Escherweg 2
 *    D-26121 Oldenburg, Germany
 *
 *  THIS SOFTWARE IS MADE AVAILABLE,  AS IS,  AND OFFIS MAKES NO  WARRANTY
 *  REGARDING  THE  SOFTWARE,  ITS  PERFORMANCE,  ITS  MERCHANTABILITY  OR
 *  FITNESS FOR ANY PARTICULAR USE, FREEDOM FROM ANY COMPUTER DISEASES  OR
 *  ITS CONFORMITY TO ANY SPECIFICATION. THE ENTIRE RISK AS TO QUALITY AND
 *  PERFORMANCE OF THE SOFTWARE IS WITH THE USER.
 *
 *  Module:  dcmqrdb
 *
 *  Author:  Marco Eichelberg
 *
 *  Purpose: class DcmQueryRetrieveStoreContext
 *
 *  Last Update:      $Author: joergr $
 *  Update Date:      $Date: 2009-12-02 16:21:18 $
 *  CVS/RCS Revision: $Revision: 1.6 $
 *  Status:           $State: Exp $
 *
 *  CVS/RCS Log at end of file
 *
 */

#include "dcmtk/config/osconfig.h"    /* make sure OS specific configuration is included first */
#include "dcmtk/dcmqrdb/dcmqrcbs.h"

#include "dcmtk/dcmqrdb/dcmqrcnf.h"
#include "dcmtk/dcmdata/dcdeftag.h"
#include "dcmtk/dcmqrdb/dcmqropt.h"
#include "dcmtk/dcmnet/diutil.h"
#include "dcmtk/dcmdata/dcfilefo.h"
#include "dcmtk/dcmqrdb/dcmqrdbs.h"
#include "dcmtk/dcmqrdb/dcmqrdbi.h"

#include "dcmtk/dcmjpeg/djdecode.h"  /* for dcmjpeg decoders */
#include "dcmtk/dcmjpeg/djencode.h"  /* for dcmjpeg encoders */
#include "/usr/local/include/fmjpeg2k/djencode.h"  /* for jpeg2000 encoders */
#include "/usr/local/include/fmjpeg2k/djrparam.h"  /* for jpeg2000 encoders */
#include "dcmtk/dcmjpeg/djrplol.h"   /* for DJ_RPLossless */
#include "dcmtk/dcmjpeg/djrploss.h"  /* for DJ_RPLossy */
#include "dcmtk/dcmjpeg/dipijpeg.h"  /* for dcmimage JPEG plugin */
#include "dcmtk/dcmimage/diregist.h"  /* include to support color images */
#include <dlfcn.h>

void DcmQueryRetrieveStoreContext::updateDisplay(T_DIMSE_StoreProgress * progress)
{
  // We can't use oflog for the pdu output, but we use a special logger for
  // generating this output. If it is set to level "INFO" we generate the
  // output, if it's set to "DEBUG" then we'll assume that there is debug output
  // generated for each PDU elsewhere.
  OFLogger progressLogger = OFLog::getLogger("dcmtk.dcmqrdb.progress");
  if (progressLogger.getChainedLogLevel() == OFLogger::INFO_LOG_LEVEL)
  {
    switch (progress->state)
    {
      case DIMSE_StoreBegin:
        printf("RECV: ");
        break;
      case DIMSE_StoreEnd:
        printf("\n");
        break;
      default:
        putchar('.');
        break;
    }
    fflush(stdout);
  }
}


void DcmQueryRetrieveStoreContext::saveImageToDB(
    T_DIMSE_C_StoreRQ *req,             /* original store request */
    const char *imageFileName,
    /* out */
    T_DIMSE_C_StoreRSP *rsp,            /* final store response */
    DcmDataset **stDetail)
{
    OFCondition dbcond = EC_Normal;
    DcmQueryRetrieveDatabaseStatus dbStatus(STATUS_Success);
    
    // TODO: get the storage rules
    // for the current sender.

    /* Store image */
    if (options_.ignoreStoreData_) {
        rsp->DimseStatus = STATUS_Success;
        *stDetail = NULL;
        return; /* nothing else to do */
    }

    if (status == STATUS_Success)
    {
        dbcond = dbHandle.storeRequest(
            req->AffectedSOPClassUID, req->AffectedSOPInstanceUID,
            imageFileName, &dbStatus);
        if (dbcond.bad())
        {
            OFString temp_str;
            DCMQRDB_ERROR("storeSCP: Database: storeRequest Failed (" <<
               DU_cstoreStatusString(dbStatus.status()) << ")\n" << DimseCondition::dump(temp_str, dbcond));
        }
        status = dbStatus.status();
    }
    rsp->DimseStatus = status;
    *stDetail = dbStatus.extractStatusDetail();
}

void DcmQueryRetrieveStoreContext::writeToFile(
    DcmFileFormat *ff,
    const char* fname,
    T_DIMSE_C_StoreRSP *rsp)
{
    // ------------ NUEVO
    OFBool opt_useLosslessProcess = OFTrue;
    // JPEG-LS options
    OFCmdUnsignedInt opt_nearlossless_psnr = 0;
    OFBool opt_prefer_cooked = OFFalse;
    // encapsulated pixel data encoding options
    OFCmdUnsignedInt opt_fragmentSize = 0; // 0=unlimited
    OFBool           opt_createOffsetTable = OFTrue;
    J2K_UIDCreation  opt_uidcreation = EJ2KUC_default;
    OFBool           opt_secondarycapture = OFFalse;
    // output options
    E_GrpLenEncoding opt_oglenc = EGL_recalcGL;
    E_EncodingType opt_oenctype = EET_ExplicitLength;
    E_PaddingEncoding opt_opadenc = EPD_noChange;
    OFCmdUnsignedInt opt_filepad = 0;
    OFCmdUnsignedInt opt_itempad = 0;

    OFCmdUnsignedInt opt_cblkwidth = 64;
    OFCmdUnsignedInt opt_cblkheight = 64; 
    OFBool opt_use_custom_options = OFFalse;
    //E_TransferSyntax opt_oxfer = EXS_JPEGProcess14SV1;
    E_TransferSyntax opt_oxfer = EXS_JPEG2000LosslessOnly;

          COUT << OFendl << "External libraries used:" << OFendl;
          COUT << "- " << FMJP2KEncoderRegistration::getLibraryVersionString() << OFendl;

    // register global decompression codecs
    DJDecoderRegistration::registerCodecs();

    // register global compression codecs
    FMJP2KEncoderRegistration::registerCodecs(opt_use_custom_options,
      OFstatic_cast(Uint16, opt_cblkwidth), OFstatic_cast(Uint16, opt_cblkheight),
      opt_prefer_cooked, opt_fragmentSize, opt_createOffsetTable,
      opt_uidcreation, opt_secondarycapture);

    //DJEncoderRegistration::registerCodecs();
   
    DcmDataset *dataset = ff->getDataset();

    DCMQRDB_INFO("--- Chequeando transfer syntax original ---");

    DcmXfer original_xfer(dataset->getOriginalXfer());
    if (original_xfer.isEncapsulated())
    {
      // TODO: first check if the file has the same transfer syntax as the to be stored
      // the uncompression is not necessary.
      DCMQRDB_INFO("DICOM file is already compressed, converting to uncompressed transfer syntax first");
      if (EC_Normal != dataset->chooseRepresentation(EXS_LittleEndianExplicit, NULL))
      {
        DCMQRDB_ERROR("no conversion from compressed original to uncompressed transfer syntax possible!");
        rsp->DimseStatus = STATUS_STORE_Refused_OutOfResources;
        return;
      }
    }

    // TODO: here the Representation is forced to JP2K,
    // this must be configured in the database,
    // depending on Sender AETitle and Modality.
    // for example: const DcmRepresentationParameter *rp = &getRepresentationParameter()

    //create representation parameter
    FMJP2KRepresentationParameter rp(OFstatic_cast(Uint16, opt_nearlossless_psnr), opt_useLosslessProcess);
    DcmXfer opt_oxferSyn(opt_oxfer);

    // create representation parameters for lossy and lossless
    //DJ_RPLossless rp_lossless(1, 0);
    //const DcmRepresentationParameter *rp = &rp_lossless;

    dataset->chooseRepresentation(opt_oxfer, &rp);
    if (dataset->canWriteXfer(opt_oxfer))
    {
      DCMQRDB_INFO("Output transfer syntax " << opt_oxferSyn.getXferName() << " can be written");
      DCMQRDB_INFO("creating output file " << fname);

      ProcessDataset(ff);

      OFCondition cond = ff->saveFile(fname, opt_oxfer, options_.sequenceType_,
          options_.groupLength_, options_.paddingType_, (Uint32)options_.filepad_,
          (Uint32)options_.itempad_, (options_.useMetaheader_) ? EWM_fileformat : EWM_dataset);
      if (cond.bad())
      {
        DCMQRDB_ERROR("storescp: Cannot write image file: " << fname);
        rsp->DimseStatus = STATUS_STORE_Refused_OutOfResources;
      }

    } else {
      DCMQRDB_ERROR("no conversion to transfer syntax " << opt_oxferSyn.getXferName() << " possible!");
      rsp->DimseStatus = STATUS_STORE_Refused_OutOfResources;
    }

    // deregister global codecs
    DJDecoderRegistration::cleanup();
    DJEncoderRegistration::cleanup();
}

void DcmQueryRetrieveStoreContext::checkRequestAgainstDataset(
    T_DIMSE_C_StoreRQ *req,     /* original store request */
    const char* fname,          /* filename of dataset */
    DcmDataset *dataSet,        /* dataset to check */
    T_DIMSE_C_StoreRSP *rsp,    /* final store response */
    OFBool uidPadding)          /* correct UID passing */
{
    DcmFileFormat ff;

    if (dataSet == NULL)
    {
      ff.loadFile(fname);
      dataSet = ff.getDataset();
    }

    /* which SOP class and SOP instance ? */
    DIC_UI sopClass;
    DIC_UI sopInstance;

    if (!DU_findSOPClassAndInstanceInDataSet(dataSet, sopClass, sopInstance, uidPadding))
    {
        DCMQRDB_ERROR("Bad image file: " << fname);
        rsp->DimseStatus = STATUS_STORE_Error_CannotUnderstand;
    } else if (strcmp(sopClass, req->AffectedSOPClassUID) != 0) {
        rsp->DimseStatus = STATUS_STORE_Error_DataSetDoesNotMatchSOPClass;
    } else if (strcmp(sopInstance, req->AffectedSOPInstanceUID) != 0) {
        rsp->DimseStatus = STATUS_STORE_Error_DataSetDoesNotMatchSOPClass;
    }
}

/* plugin for pre-processing dataset */
// TODO: replace for javascript/lua scripts
void DcmQueryRetrieveStoreContext::ProcessDataset(DcmFileFormat * ff)
{
    DCMQRDB_INFO("Opening libpreprocess.so...");
    void* lhandle = dlopen("./libpreprocess.so", RTLD_NOW);

    if (lhandle) {
      DCMQRDB_INFO("libpreprocess.so is open.");
      typedef void (*processDataset)(DcmFileFormat *);
      processDataset lprocessDset = (processDataset) dlsym(lhandle, "processDataset");
      if (!lprocessDset) {
         DCMQRDB_INFO("Cannot load symbol 'processDataset': " << dlerror());
         dlclose(lhandle);
         return;
      }

     DCMQRDB_INFO("Calling ProcessDataset...");
     lprocessDset(ff);
     DCMQRDB_INFO("ProcessDataset finished ok.");

      //ff->saveFile("/home/leonardo/Desarrollo/dcmtk/DCMQRSCP/salida.dcm");
    }
    else
    {
      char * error;
      error = dlerror();
      DCMQRDB_INFO("Cannot open open libpreprocess.so (" << error);
    }

    //dlclose(lhandle);
}

void DcmQueryRetrieveStoreContext::callbackHandler(
    /* in */
    T_DIMSE_StoreProgress *progress,    /* progress state */
    T_DIMSE_C_StoreRQ *req,             /* original store request */
    char *imageFileName,       /* being received into */
    DcmDataset **imageDataSet, /* being received into */
    /* out */
    T_DIMSE_C_StoreRSP *rsp,            /* final store response */
    DcmDataset **stDetail)
{
    updateDisplay(progress);

    if (progress->state == DIMSE_StoreEnd) {

        if (!options_.ignoreStoreData_ && rsp->DimseStatus == STATUS_Success) {
            if ((imageDataSet)&&(*imageDataSet)) {
                checkRequestAgainstDataset(req, NULL, *imageDataSet, rsp, correctUIDPadding);
            } else {
                checkRequestAgainstDataset(req, imageFileName, NULL, rsp, correctUIDPadding);
            }
        }
        
        if (!options_.ignoreStoreData_ && rsp->DimseStatus == STATUS_Success) {
            // TODO: here the connection to database should be established
            if ((imageDataSet)&&(*imageDataSet)) {
                writeToFile(dcmff, fileName, rsp);
            }
            if (rsp->DimseStatus == STATUS_Success) {
                saveImageToDB(req, fileName, rsp, stDetail);
            }
            // TODO: disconnect from database
        }

        OFString str;
        if (rsp->DimseStatus != STATUS_Success)
            DCMQRDB_WARN("NOTICE: StoreSCP:\n" << DIMSE_dumpMessage(str, *rsp, DIMSE_OUTGOING));
        else
            DCMQRDB_INFO("Sending:\n" << DIMSE_dumpMessage(str, *rsp, DIMSE_OUTGOING));
        status = rsp->DimseStatus;
    }
}


/*
 * CVS Log
 * $Log: dcmqrcbs.cc,v $
 * Revision 1.6  2009-12-02 16:21:18  joergr
 * Slightly modified output of progress bar.
 *
 * Revision 1.5  2009-11-24 10:10:42  uli
 * Switched to logging mechanism provided by the "new" oflog module.
 *
 * Revision 1.4  2009-08-21 09:53:52  joergr
 * Added parameter 'writeMode' to save/write methods which allows for specifying
 * whether to write a dataset or fileformat as well as whether to update the
 * file meta information or to create a new file meta information header.
 *
 * Revision 1.3  2005/12/15 12:38:06  joergr
 * Removed naming conflicts.
 *
 * Revision 1.2  2005/12/08 15:47:07  meichel
 * Changed include path schema for all DCMTK header files
 *
 * Revision 1.1  2005/03/30 13:34:53  meichel
 * Initial release of module dcmqrdb that will replace module imagectn.
 *   It provides a clear interface between the Q/R DICOM front-end and the
 *   database back-end. The imagectn code has been re-factored into a minimal
 *   class structure.
 *
 *
 */
