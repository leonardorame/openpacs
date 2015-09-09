/*
 *
 *  Copyright (C) 1993-2011, Griensu S.A.
 *
 *  Module:  dcmqrdb
 *
 *  Author:  Leonardo M. Ramé
 *
 *  Purpose: classes DcmQueryRetrieveSqlDatabaseHandle, DcmQueryRetrieveSqlDatabaseHandleFactory
 *
 */

#include "dcmtk/config/osconfig.h"    /* make sure OS specific configuration is included first */

BEGIN_EXTERN_C

#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
END_EXTERN_C
#define INCLUDE_CCTYPE
#define INCLUDE_CSTDARG
#include "dcmtk/ofstd/ofstdinc.h"


#include "dcmtk/dcmqrdb/dcmqrdbs.h"
#include "dcmtk/dcmqrdb/dcmqrcnf.h"
#include "dcmtk/dcmqrdb/dcmqridx.h"
#include "dcmtk/dcmqrdb/dcmdefines.h"
#include "dcmtk/dcmnet/diutil.h"
#include "dcmtk/dcmdata/dcfilefo.h"
#include "dcmtk/dcmdata/dcdeftag.h"
#include "dcmtk/dcmdata/dcuid.h"
#include "dcmtk/ofstd/ofstd.h"
#include "dcmtk/dcmqrdbx/dcmqrdbq.h"
/*Pasini Nicolas*/
#include "dcmtk/dcmimgle/dcmimage.h"
// JPEG parameters
#include "djencode.h"      /* for DJLSEncoderRegistration */
#include "dcmtk/dcmjpeg/djdecode.h"      /* for dcmjpeg decoders */
#include "dcmtk/dcmjpeg/dipijpeg.h"      /* for dcmimage JPEG plugin */
// RGB images
#include "dcmtk/dcmimage/diregist.h"
#include <dlfcn.h>
#include <sstream>

typedef bool(*validateFingerPrintAlg2_t)(const char * AKey);
typedef std::string(*createFingerPrintAlg1_t)(std::string Avar);

bool estado(std::string AKey);
std::string getFingerPrint();
OFCmdUnsignedInt    opt_quality = 90;                 /* default: 90% JPEG quality */
E_SubSampling       opt_sampling = ESS_422;           /* default: 4:2:2 sub-sampling */

/*Nicolas*/

makeOFConditionConst(DcmQRSqlDatabaseError, OFM_dcmqrdb, 1, OF_error, "DcmQR Sql Database Error");
const OFCondition SqlDatabaseError(DcmQRSqlDatabaseError); 

makeOFConditionConst(DcmQRSqlDatabaseJPGError, OFM_dcmqrdb, 2, OF_error, "Error al generar jpg");
const OFCondition SqlDatabaseJPEGError(DcmQRSqlDatabaseError); 

const char * CLIENT_ENCODING = "SET client_encoding to 'ISO_8859_1'";

/* ========================= static data ========================= */

/**** The TbFindAttr table contains the description of tags (keys) supported
 **** by the DB Module.
 **** Tags described here have to be present in the Index Record file.
 **** The order is unsignificant.
 ****
 **** Each element of this table is described by
 ****           The tag value
 ****           The level of this tag (from patient to image)
 ****           The Key Type (only UNIQUE_KEY values is used)
 ****           The key matching type, specifiing which type of
 ****                   matching should be performed. The OTHER_CLASS
 ****                   value specifies that only strict comparison is applied.
 ****
 **** This table and the IndexRecord structure should contain at least
 **** all Unique and Required keys.
 ***/

static const DB_FindAttr TbFindAttr [] = {
        DB_FindAttr( DCM_PatientBirthDate ,                    PATIENT_LEVEL,  OPTIONAL_KEY,   DATE_CLASS      ),
        DB_FindAttr( DCM_PatientSex,                           PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_PatientName,                          PATIENT_LEVEL,  REQUIRED_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_PatientID,                             PATIENT_LEVEL,  UNIQUE_KEY,     STRING_CLASS    ),
        DB_FindAttr( DCM_PatientBirthTime,                     PATIENT_LEVEL,  OPTIONAL_KEY,   TIME_CLASS      ),
        DB_FindAttr( DCM_OtherPatientIDs,                       PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_OtherPatientNames,                     PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_EthnicGroup,                           PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_PatientComments,                       PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NumberOfPatientRelatedStudies,         PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NumberOfPatientRelatedSeries,          PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NumberOfPatientRelatedInstances,       PATIENT_LEVEL,  OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_StudyDate,                             STUDY_LEVEL,    REQUIRED_KEY,   DATE_CLASS      ),
        DB_FindAttr( DCM_StudyTime,                             STUDY_LEVEL,    REQUIRED_KEY,   TIME_CLASS      ),
        DB_FindAttr( DCM_StudyID,                               STUDY_LEVEL,    REQUIRED_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_AccessionNumber,                       STUDY_LEVEL,    REQUIRED_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_ReferringPhysiciansName,               STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_StudyDescription,                      STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NameOfPhysiciansReadingStudy,          STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_StudyInstanceUID,                      STUDY_LEVEL,    UNIQUE_KEY,     UID_CLASS       ),
        DB_FindAttr( DCM_RETIRED_OtherStudyNumbers,             STUDY_LEVEL,    OPTIONAL_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_AdmittingDiagnosesDescription,         STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_PatientAge,                           STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_PatientSize,                          STUDY_LEVEL,    OPTIONAL_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_PatientWeight,                        STUDY_LEVEL,    OPTIONAL_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_Occupation,                            STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_AdditionalPatientHistory,              STUDY_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NumberOfStudyRelatedSeries,            STUDY_LEVEL,    OPTIONAL_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_NumberOfStudyRelatedInstances,         STUDY_LEVEL,    OPTIONAL_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_InstitutionName,                       STUDY_LEVEL,    OPTIONAL_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_SeriesNumber,                          SERIE_LEVEL,    REQUIRED_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_SeriesInstanceUID,                     SERIE_LEVEL,    UNIQUE_KEY,     UID_CLASS       ),
        DB_FindAttr( DCM_Modality,                              SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_BodyPartExamined,                      SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_SeriesDate,                            SERIE_LEVEL,    OPTIONAL_KEY,   DATE_CLASS      ),
        DB_FindAttr( DCM_SeriesTime,                            SERIE_LEVEL,    OPTIONAL_KEY,   TIME_CLASS      ),
        DB_FindAttr( DCM_SeriesDescription,                     SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_ProtocolName,                          SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_PatientPosition,                       SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_SeriesDescription,                     SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NumberOfSeriesRelatedInstances,        SERIE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_Rows,                                  IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_Columns,                               IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_InstanceNumber,                        IMAGE_LEVEL,    REQUIRED_KEY,   OTHER_CLASS     ),
        DB_FindAttr( DCM_ImageType,                             IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_SOPClassUID,                           IMAGE_LEVEL,    UNIQUE_KEY,     UID_CLASS       ),
        DB_FindAttr( DCM_AcquisitionDate,                       IMAGE_LEVEL,    OPTIONAL_KEY,   DATE_CLASS      ),
        DB_FindAttr( DCM_AcquisitionTime,                       IMAGE_LEVEL,    OPTIONAL_KEY,   TIME_CLASS      ),
        DB_FindAttr( DCM_AcquisitionNumber,                     IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_ContentDate,                           IMAGE_LEVEL,    OPTIONAL_KEY,   DATE_CLASS      ),
        DB_FindAttr( DCM_ContentTime,                           IMAGE_LEVEL,    OPTIONAL_KEY,   TIME_CLASS      ),
        DB_FindAttr( DCM_ImagePositionPatient,                  IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_ImageOrientationPatient,               IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_FrameOfReferenceUID,                   IMAGE_LEVEL,    OPTIONAL_KEY,   UID_CLASS       ),
        DB_FindAttr( DCM_SliceLocation,                         IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_NumberOfFrames,                        IMAGE_LEVEL,    OPTIONAL_KEY,   STRING_CLASS    ),
        DB_FindAttr( DCM_SOPInstanceUID,                        IMAGE_LEVEL,    UNIQUE_KEY,     UID_CLASS       )
};

/**** The NbFindAttr variable contains the length of the TbFindAttr table
 ***/

static int NbFindAttr = ((sizeof (TbFindAttr)) / (sizeof (TbFindAttr [0])));
/****
imageFileName = ***/

/* ========================= static functions ========================= */

static char *DB_strdup(const char* str)
{
    if (str == NULL) return NULL;
    char* s = (char*)malloc(strlen(str)+1);
    strcpy(s, str);
    return s;
}

/************
**      Add UID in Index Record to the UID found list
 */


static void DB_UIDAddFound (
                DB_Private_Handle       *phandle,
                IdxRecord               *idxRec
                )
{
    DB_UidList *plist ;

    plist = (DB_UidList *) malloc (sizeof (DB_UidList)) ;
    if (plist == NULL) {
        DCMQRDB_ERROR("DB_UIDAddFound: out of memory");
        return;
    }
    plist->next = phandle->uidList ;
    plist->patient = NULL ;
    plist->study = NULL ;
    plist->serie = NULL ;
    plist->image = NULL ;

    if ((int)phandle->queryLevel >= PATIENT_LEVEL)
        plist->patient = DB_strdup ((char *) idxRec->PatientID) ;
    if ((int)phandle->queryLevel >= STUDY_LEVEL)
        plist->study = DB_strdup ((char *) idxRec->StudyInstanceUID) ;
    if ((int)phandle->queryLevel >= SERIE_LEVEL)
        plist->serie = DB_strdup ((char *) idxRec->SeriesInstanceUID) ;
    if ((int)phandle->queryLevel >= IMAGE_LEVEL)
        plist->image = DB_strdup ((char *) idxRec->SOPInstanceUID) ;

    phandle->uidList = plist ;
}


/************
 *      Initializes addresses in an IdxRecord
 */

static void DB_IdxInitRecord (IdxRecord *idx, int linksOnly)
{
    /*
    if (! linksOnly)
    {
        idx -> param[RECORDIDX_PatientBirthDate]. XTag = DCM_PatientBirthDate  ;
        idx -> param[RECORDIDX_PatientBirthDate]. ValueLength = DA_MAX_LENGTH  ;
        idx -> PatientBirthDate[0] = '\0' ;
        idx -> param[RECORDIDX_PatientSex]. XTag = DCM_PatientSex  ;
        idx -> param[RECORDIDX_PatientSex]. ValueLength = CS_MAX_LENGTH  ;
        idx -> PatientSex[0] = '\0' ;
        idx -> param[RECORDIDX_PatientName]. XTag = DCM_PatientName  ;
        idx -> param[RECORDIDX_PatientName]. ValueLength = PN_MAX_LENGTH  ;
        idx -> PatientName[0] = '\0' ;
        idx -> param[RECORDIDX_PatientID]. XTag = DCM_PatientID  ;
        idx -> param[RECORDIDX_PatientID]. ValueLength = LO_MAX_LENGTH  ;
        idx -> PatientID[0] = '\0' ;
        idx -> param[RECORDIDX_PatientBirthTime]. XTag = DCM_PatientBirthTime  ;
        idx -> param[RECORDIDX_PatientBirthTime]. ValueLength = TM_MAX_LENGTH  ;
        idx -> PatientBirthTime[0] = '\0' ;
        idx -> param[RECORDIDX_OtherPatientIDs]. XTag = DCM_OtherPatientIDs  ;
        idx -> param[RECORDIDX_OtherPatientIDs]. ValueLength = LO_MAX_LENGTH  ;
        idx -> OtherPatientIDs[0] = '\0' ;
        idx -> param[RECORDIDX_OtherPatientNames]. XTag = DCM_OtherPatientNames  ;
        idx -> param[RECORDIDX_OtherPatientNames]. ValueLength = PN_MAX_LENGTH  ;
        idx -> OtherPatientNames[0] = '\0' ;
        idx -> param[RECORDIDX_EthnicGroup]. XTag = DCM_EthnicGroup  ;
        idx -> param[RECORDIDX_EthnicGroup]. ValueLength = SH_MAX_LENGTH  ;
        idx -> EthnicGroup[0] = '\0' ;
        idx -> param[RECORDIDX_NumberofPatientRelatedStudies]. XTag = DCM_NumberOfPatientRelatedStudies  ;
        idx -> param[RECORDIDX_NumberofPatientRelatedStudies]. ValueLength = IS_MAX_LENGTH  ;
        idx -> NumberofPatientRelatedStudies[0] = '\0' ;
        idx -> param[RECORDIDX_NumberofPatientRelatedSeries]. XTag = DCM_NumberOfPatientRelatedSeries  ;
        idx -> param[RECORDIDX_NumberofPatientRelatedSeries]. ValueLength = IS_MAX_LENGTH  ;
        idx -> NumberofPatientRelatedSeries[0] = '\0' ;
        idx -> param[RECORDIDX_NumberofPatientRelatedInstances]. XTag = DCM_NumberOfPatientRelatedInstances  ;
        idx -> param[RECORDIDX_NumberofPatientRelatedInstances]. ValueLength = IS_MAX_LENGTH  ;
        idx -> NumberofPatientRelatedInstances[0] = '\0' ;
        idx -> param[RECORDIDX_StudyDate]. XTag = DCM_StudyDate  ;
        idx -> param[RECORDIDX_StudyDate]. ValueLength = DA_MAX_LENGTH  ;
        idx -> StudyDate[0] = '\0' ;
        idx -> param[RECORDIDX_StudyTime]. XTag = DCM_StudyTime  ;
        idx -> param[RECORDIDX_StudyTime]. ValueLength = TM_MAX_LENGTH  ;
        idx -> StudyTime[0] = '\0' ;
        idx -> param[RECORDIDX_StudyID]. XTag = DCM_StudyID  ;
        idx -> param[RECORDIDX_StudyID]. ValueLength = CS_MAX_LENGTH  ;
        idx -> StudyID[0] = '\0' ;
        idx -> param[RECORDIDX_StudyDescription]. XTag = DCM_StudyDescription  ;
        idx -> param[RECORDIDX_StudyDescription]. ValueLength = LO_MAX_LENGTH  ;
        idx -> StudyDescription[0] = '\0' ;
        idx -> param[RECORDIDX_NameOfPhysiciansReadingStudy]. XTag = DCM_NameOfPhysiciansReadingStudy  ;
        idx -> param[RECORDIDX_NameOfPhysiciansReadingStudy]. ValueLength = PN_MAX_LENGTH  ;
        idx -> NameOfPhysiciansReadingStudy[0] = '\0' ;
        idx -> param[RECORDIDX_AccessionNumber]. XTag = DCM_AccessionNumber;
        idx -> param[RECORDIDX_AccessionNumber]. ValueLength = CS_MAX_LENGTH ;
        idx -> AccessionNumber[0] = '\0' ;
        idx -> param[RECORDIDX_ReferringPhysiciansName]. XTag = DCM_ReferringPhysiciansName  ;
        idx -> param[RECORDIDX_ReferringPhysiciansName]. ValueLength = PN_MAX_LENGTH ;
        idx -> ReferringPhysiciansName[0] = '\0' ;
        idx -> param[RECORDIDX_ProcedureDescription]. XTag = DCM_StudyDescription  ;
        idx -> param[RECORDIDX_ProcedureDescription]. ValueLength = LO_MAX_LENGTH ;
        idx -> ProcedureDescription[0] = '\0' ;
        idx -> param[RECORDIDX_AttendingPhysiciansName]. XTag = DCM_NameOfPhysiciansReadingStudy  ;
        idx -> param[RECORDIDX_AttendingPhysiciansName]. ValueLength = PN_MAX_LENGTH ;
        idx -> AttendingPhysiciansName[0] = '\0' ;
        idx -> param[RECORDIDX_StudyInstanceUID]. XTag = DCM_StudyInstanceUID  ;
        idx -> param[RECORDIDX_StudyInstanceUID]. ValueLength = UI_MAX_LENGTH ;
        idx -> StudyInstanceUID[0] = '\0' ;
        idx -> param[RECORDIDX_OtherStudyNumbers]. XTag = DCM_RETIRED_OtherStudyNumbers  ;
        idx -> param[RECORDIDX_OtherStudyNumbers]. ValueLength = IS_MAX_LENGTH ;
        idx -> OtherStudyNumbers[0] = '\0' ;
        idx -> param[RECORDIDX_AdmittingDiagnosesDescription]. XTag = DCM_AdmittingDiagnosesDescription  ;
        idx -> param[RECORDIDX_AdmittingDiagnosesDescription]. ValueLength = LO_MAX_LENGTH ;
        idx -> AdmittingDiagnosesDescription[0] = '\0' ;
        idx -> param[RECORDIDX_PatientAge]. XTag = DCM_PatientAge  ;
        idx -> param[RECORDIDX_PatientAge]. ValueLength = AS_MAX_LENGTH ;
        idx -> PatientAge[0] = '\0' ;
        idx -> param[RECORDIDX_PatientSize]. XTag = DCM_PatientSize  ;
        idx -> param[RECORDIDX_PatientSize]. ValueLength = DS_MAX_LENGTH ;
        idx -> PatientSize[0] = '\0' ;
        idx -> param[RECORDIDX_PatientWeight]. XTag = DCM_PatientWeight  ;
        idx -> param[RECORDIDX_PatientWeight]. ValueLength = DS_MAX_LENGTH ;
        idx -> PatientWeight[0] = '\0' ;
        idx -> param[RECORDIDX_Occupation]. XTag = DCM_Occupation  ;
        idx -> param[RECORDIDX_Occupation]. ValueLength = SH_MAX_LENGTH ;
        idx -> Occupation[0] = '\0' ;
        idx -> param[RECORDIDX_NumberofStudyRelatedSeries]. XTag = DCM_NumberOfStudyRelatedSeries  ;
        idx -> param[RECORDIDX_NumberofStudyRelatedSeries]. ValueLength = IS_MAX_LENGTH ;
        idx -> NumberofStudyRelatedSeries[0] = '\0' ;
        idx -> param[RECORDIDX_NumberofStudyRelatedInstances]. XTag = DCM_NumberOfStudyRelatedInstances  ;
        idx -> param[RECORDIDX_NumberofStudyRelatedInstances]. ValueLength = IS_MAX_LENGTH ;
        idx -> NumberofStudyRelatedInstances[0] = '\0' ;
        idx -> param[RECORDIDX_SeriesNumber]. XTag = DCM_SeriesNumber  ;
        idx -> param[RECORDIDX_SeriesNumber]. ValueLength = IS_MAX_LENGTH ;
        idx -> SeriesNumber[0] = '\0' ;
        idx -> param[RECORDIDX_SeriesInstanceUID]. XTag = DCM_SeriesInstanceUID  ;
        idx -> param[RECORDIDX_SeriesInstanceUID]. ValueLength = UI_MAX_LENGTH ;
        idx -> SeriesInstanceUID[0] = '\0' ;
        idx -> param[RECORDIDX_Modality]. XTag = DCM_Modality  ;
        idx -> param[RECORDIDX_Modality]. ValueLength = CS_MAX_LENGTH ;
        idx -> ImageNumber[0] = '\0' ;
        idx -> param[RECORDIDX_ImageNumber]. XTag = DCM_InstanceNumber  ;
        idx -> param[RECORDIDX_ImageNumber]. ValueLength = IS_MAX_LENGTH ;
        idx -> ImageNumber[0] = '\0' ;
        idx -> param[RECORDIDX_SOPInstanceUID]. XTag = DCM_SOPInstanceUID  ;
        idx -> param[RECORDIDX_SOPInstanceUID]. ValueLength = UI_MAX_LENGTH ;
        idx -> SOPInstanceUID[0] = '\0' ;
        idx -> param[RECORDIDX_SeriesDate]. XTag = DCM_SeriesDate;
        idx -> param[RECORDIDX_SeriesDate]. ValueLength = DA_MAX_LENGTH ;
        idx -> SeriesDate[0] = '\0'     ;
        idx -> param[RECORDIDX_SeriesTime]. XTag = DCM_SeriesTime;
        idx -> param[RECORDIDX_SeriesTime]. ValueLength = TM_MAX_LENGTH ;
        idx -> SeriesTime[0] = '\0'     ;
        idx -> param[RECORDIDX_SeriesDescription]. XTag = DCM_SeriesDescription  ;
        idx -> param[RECORDIDX_SeriesDescription]. ValueLength = LO_MAX_LENGTH ;
        idx -> SeriesDescription[0] = '\0'      ;
        //g-n
        idx -> param[RECORDIDX_BodyPartExamined]. XTag = DCM_BodyPartExamined  ;
        idx -> param[RECORDIDX_BodyPartExamined]. ValueLength = LO_MAX_LENGTH ;
        idx -> BodyPartExamined[0] = '\0'      ;

        //g-n
        idx -> param[RECORDIDX_ProtocolName]. XTag = DCM_ProtocolName  ;
        idx -> param[RECORDIDX_ProtocolName]. ValueLength = LO_MAX_LENGTH ;
        idx -> ProtocolName[0] = '\0'   ;
        idx -> param[RECORDIDX_OperatorsName ]. XTag = DCM_OperatorsName  ;
        idx -> param[RECORDIDX_OperatorsName ]. ValueLength = PN_MAX_LENGTH ;
        idx -> OperatorsName[0] = '\0';
        idx -> param[RECORDIDX_PerformingPhysiciansName]. XTag = DCM_PerformingPhysiciansName  ;
        idx -> param[RECORDIDX_PerformingPhysiciansName]. ValueLength = PN_MAX_LENGTH ;
        idx -> PerformingPhysiciansName[0] = '\0';
        idx -> param[RECORDIDX_PresentationLabel]. XTag = DCM_ContentLabel  ;
        idx -> param[RECORDIDX_PresentationLabel]. ValueLength = CS_LABEL_MAX_LENGTH ;
        idx -> PresentationLabel[0] = '\0';
    }
    idx -> param[RECORDIDX_PatientBirthDate]. PValueField = (char *)idx -> PatientBirthDate ;
    idx -> param[RECORDIDX_PatientSex]. PValueField = (char *)idx -> PatientSex ;
    idx -> param[RECORDIDX_PatientName]. PValueField = (char *)idx -> PatientName ;
    idx -> param[RECORDIDX_PatientID]. PValueField = (char *)idx -> PatientID ;
    idx -> param[RECORDIDX_PatientBirthTime]. PValueField = (char *)idx -> PatientBirthTime ;
    idx -> param[RECORDIDX_OtherPatientIDs]. PValueField = (char *)idx -> OtherPatientIDs ;
    idx -> param[RECORDIDX_OtherPatientNames]. PValueField = (char *)idx -> OtherPatientNames ;
    idx -> param[RECORDIDX_EthnicGroup]. PValueField = (char *)idx -> EthnicGroup ;
    idx -> param[RECORDIDX_NumberofPatientRelatedStudies]. PValueField = (char *)idx -> NumberofPatientRelatedStudies ;
    idx -> param[RECORDIDX_NumberofPatientRelatedSeries]. PValueField = (char *) idx -> NumberofPatientRelatedSeries ;
    idx -> param[RECORDIDX_NumberofPatientRelatedInstances]. PValueField = (char *) idx -> NumberofPatientRelatedInstances ;
    idx -> param[RECORDIDX_StudyDate]. PValueField = (char *) idx -> StudyDate ;
    idx -> param[RECORDIDX_StudyTime]. PValueField = (char *) idx -> StudyTime ;
    idx -> param[RECORDIDX_StudyID]. PValueField = (char *) idx -> StudyID ;
    idx -> param[RECORDIDX_StudyDescription]. PValueField = (char *) idx -> StudyDescription ;
    idx -> param[RECORDIDX_NameOfPhysiciansReadingStudy]. PValueField = (char *) idx ->NameOfPhysiciansReadingStudy;
    idx -> param[RECORDIDX_AccessionNumber]. PValueField = (char *) idx -> AccessionNumber ;
    idx -> param[RECORDIDX_ReferringPhysiciansName]. PValueField = (char *) idx -> ReferringPhysiciansName ;
    idx -> param[RECORDIDX_ProcedureDescription]. PValueField = (char *) idx -> ProcedureDescription ;
    idx -> param[RECORDIDX_AttendingPhysiciansName]. PValueField = (char *) idx -> AttendingPhysiciansName ;
    idx -> param[RECORDIDX_StudyInstanceUID]. PValueField = (char *) idx -> StudyInstanceUID ;
    idx -> param[RECORDIDX_OtherStudyNumbers]. PValueField = (char *) idx -> OtherStudyNumbers ;
    idx -> param[RECORDIDX_AdmittingDiagnosesDescription]. PValueField = (char *) idx -> AdmittingDiagnosesDescription ;
    idx -> param[RECORDIDX_PatientAge]. PValueField = (char *) idx -> PatientAge ;
    idx -> param[RECORDIDX_PatientSize]. PValueField = (char *) idx -> PatientSize ;
    idx -> param[RECORDIDX_PatientWeight]. PValueField = (char *) idx -> PatientWeight ;
    idx -> param[RECORDIDX_Occupation]. PValueField = (char *) idx -> Occupation ;
    idx -> param[RECORDIDX_NumberofStudyRelatedSeries]. PValueField = (char *) idx -> NumberofStudyRelatedSeries ;
    idx -> param[RECORDIDX_NumberofStudyRelatedInstances]. PValueField = (char *) idx -> NumberofStudyRelatedInstances ;
    idx -> param[RECORDIDX_SeriesNumber]. PValueField = (char *) idx -> SeriesNumber ;
    idx -> param[RECORDIDX_SeriesInstanceUID]. PValueField = (char *) idx -> SeriesInstanceUID ;
    idx -> param[RECORDIDX_Modality]. PValueField = (char *) idx -> Modality ;
    idx -> param[RECORDIDX_ImageNumber]. PValueField = (char *) idx -> ImageNumber ;
    idx -> param[RECORDIDX_SOPInstanceUID]. PValueField = (char *) idx -> SOPInstanceUID ;
    idx -> param[RECORDIDX_SeriesDate]. PValueField = (char *) idx -> SeriesDate ;
    idx -> param[RECORDIDX_SeriesTime]. PValueField = (char *) idx -> SeriesTime ;
    idx -> param[RECORDIDX_SeriesDescription]. PValueField = (char *) idx -> SeriesDescription ;
    //n-g
    idx -> param[RECORDIDX_BodyPartExamined]. PValueField = (char *) idx -> BodyPartExamined ;
    idx -> param[RECORDIDX_Rows]. PValueField = (char *) idx -> Rows ;
    idx -> param[RECORDIDX_Columns]. PValueField = (char *) idx -> Columns ;
    //n-g
    idx -> param[RECORDIDX_ProtocolName]. PValueField = (char *) idx -> ProtocolName ;
    idx -> param[RECORDIDX_OperatorsName ]. PValueField = (char *) idx -> OperatorsName ;
    idx -> param[RECORDIDX_PerformingPhysiciansName]. PValueField = (char *) idx -> PerformingPhysiciansName ;
    idx -> param[RECORDIDX_PresentationLabel]. PValueField = (char *) idx -> PresentationLabel ;
  */
}

/******************************
 *      Seek to a file position and do error checking
 *
 * Motivation:
 * We have had situations during demonstrations where size of the DB index file
 * has exploded.  It seems that a record is being written to a position
 * way past the end of file.
 * This seek function does some sanity error checking to try to identify
 * the problem.
 */
static long DB_lseek(int fildes, long offset, int whence)
{
    long pos;
    long curpos;
    long endpos;

    /*
    ** we should not be seeking to an offset < 0
    */
    if (offset < 0) {
        DCMQRDB_ERROR("*** DB ALERT: attempt to seek before begining of file");
    }

    /* get the current position */
    curpos = lseek(fildes, 0, SEEK_CUR);
    if (curpos < 0) {
        DCMQRDB_ERROR("DB_lseek: cannot get current position: " << strerror(errno));
        return curpos;
    }
    /* get the end of file position */
    endpos = lseek(fildes, 0, SEEK_END);
    if (endpos < 0) {
        DCMQRDB_ERROR("DB_lseek: cannot get end of file position: " << strerror(errno));
        return endpos;
    }

    /* return to current position */
    curpos = lseek(fildes, curpos, SEEK_SET);
    if (curpos < 0) {
        DCMQRDB_ERROR("DB_lseek: cannot reset current position: " << strerror(errno));
        return curpos;
    }

    /* do the requested seek */
    pos = lseek(fildes, offset, whence);
    if (pos < 0) {
        DCMQRDB_ERROR("DB_lseek: cannot seek to " << offset << ": " << strerror(errno));
        return pos;
    }

    /*
    ** print an alert if we are seeking to far
    ** what is the limit? We don't expect the index file to be
    ** larger than 32Mb
    */
    const long maxFileSize = 33554432;
    if (pos > maxFileSize) {
        DCMQRDB_ERROR("*** DB ALERT: attempt to seek beyond " << maxFileSize << " bytes");
    }

    /* print an alert if we are seeking beyond the end of file.
     * ignore when file is empty
     */
    if ((endpos > 0) && (pos > endpos)) {
        DCMQRDB_ERROR("*** DB ALERT: attempt to seek beyond end of file" << OFendl
            << "              offset=" << offset << " filesize=" << endpos);
    }

    return pos;
}

/******************************
 *      Read an Index record
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_IdxRead (int idx, IdxRecord *idxRec)
{

    /*** Goto the right index in file
    **/

    DB_lseek (handle_ -> pidx, (long) (SIZEOF_STUDYDESC + idx * SIZEOF_IDXRECORD), SEEK_SET) ;

    /*** Read the record
    **/

    if (read (handle_ -> pidx, (char *) idxRec, SIZEOF_IDXRECORD) != SIZEOF_IDXRECORD)
        return (DcmQRSqlDatabaseError) ;

    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;

    /*** Initialize record links
    **/

    DB_IdxInitRecord (idxRec, 1) ;
    return EC_Normal ;
}



/******************************
 *      Change the StudyDescRecord
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_StudyDescChange(StudyDescRecord *pStudyDesc)
{
    OFCondition cond = EC_Normal;
    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;
    if (write (handle_ -> pidx, (char *) pStudyDesc, SIZEOF_STUDYDESC) != SIZEOF_STUDYDESC) cond = DcmQRSqlDatabaseError;
    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;
    return cond ;
}

/******************************
 *      Init an Index record loop
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_IdxInitLoop(int *idx)
{
    DB_lseek (handle_ -> pidx, SIZEOF_STUDYDESC, SEEK_SET) ;
    *idx = -1 ;
    return EC_Normal ;
}

/******************************
 *      Get next Index record
 *      On return, idx is initialized with the index of the record read
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_IdxGetNext(int *idx, IdxRecord *idxRec)
{

    (*idx)++ ;
    DB_lseek (handle_ -> pidx, SIZEOF_STUDYDESC + (long)(*idx) * SIZEOF_IDXRECORD, SEEK_SET) ;
    while (read (handle_ -> pidx, (char *) idxRec, SIZEOF_IDXRECORD) == SIZEOF_IDXRECORD) {
        if (idxRec -> filename [0] != '\0') {
            DB_IdxInitRecord (idxRec, 1) ;

            return EC_Normal ;
        }
        (*idx)++ ;
    }

    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;

    return DcmQRSqlDatabaseError ;
}


/******************************
 *      Get next Index record
 *      On return, idx is initialized with the index of the record read
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_GetStudyDesc (StudyDescRecord *pStudyDesc)
{
    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;
    if ( read (handle_ -> pidx, (char *) pStudyDesc, SIZEOF_STUDYDESC) == SIZEOF_STUDYDESC )
        return EC_Normal ;

    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;

    return DcmQRSqlDatabaseError ;
}


/******************************
 *      Remove an Index record
 *      Just put a record with filename == ""
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_IdxRemove(int idx)
{
    IdxRecord   rec ;
    OFCondition cond = EC_Normal;

    DB_lseek (handle_ -> pidx, SIZEOF_STUDYDESC + (long)idx * SIZEOF_IDXRECORD, SEEK_SET) ;
    DB_IdxInitRecord (&rec, 0) ;

    rec. filename [0] = '\0' ;
    if (write (handle_ -> pidx, (char *) &rec, SIZEOF_IDXRECORD) == SIZEOF_IDXRECORD)
        cond = EC_Normal ;
    else
        cond = DcmQRSqlDatabaseError ;

    DB_lseek (handle_ -> pidx, 0L, SEEK_SET) ;

    return cond ;
}

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_lock(OFBool exclusive)
{
    int lockmode;

    if (exclusive) {
        lockmode = LOCK_EX;     /* exclusive lock */
    } else {
        lockmode = LOCK_SH;     /* shared lock */
    }
    if (dcmtk_flock(handle_->pidx, lockmode) < 0) {
        dcmtk_plockerr("DB_lock");
        return DcmQRSqlDatabaseError;
    }
    return EC_Normal;
}

OFCondition DcmQueryRetrieveSqlDatabaseHandle::DB_unlock()
{
    if (dcmtk_flock(handle_->pidx, LOCK_UN) < 0) {
        dcmtk_plockerr("DB_unlock");
        return DcmQRSqlDatabaseError;
    }
    return EC_Normal;
}

/*******************
 *    Free an element List
 */

static OFCondition DB_FreeUidList (DB_UidList *lst)
{
    if (lst == NULL) return EC_Normal;

    OFCondition cond = DB_FreeUidList (lst -> next);
    if (lst -> patient)
        free (lst -> patient);
    if (lst -> study)
        free (lst -> study);
    if (lst -> serie)
        free (lst -> serie);
    if (lst -> image)
        free (lst -> image);
    free (lst);
    return (cond);
}


/*******************
 *    Free a UID List
 */

static OFCondition DB_FreeElementList (DB_ElementList *lst)
{
    if (lst == NULL) return EC_Normal;

    OFCondition cond = DB_FreeElementList (lst -> next);
    if (lst->elem.PValueField != NULL) {
        free ((char *) lst -> elem. PValueField);
    }
    free (lst);
    return (cond);
}


/*******************
 *    Matches two strings
 */

static int DB_StringUnify  (char *pmod, char *pstr)
{
    int uni;

    if (*pmod == '\0')
    return (*pstr == '\0');

    if (  *pmod == *pstr
      || (*pmod == '?' && *pstr != '\0')
    )
    return (DB_StringUnify (pmod + 1, pstr + 1));

    if (*pmod == '*') {
    if ( *(pmod + 1) == '\0' )
        return (OFTrue);
    while (  ( (uni = DB_StringUnify (pmod + 1, pstr)) == OFFalse )
         && (*pstr != '\0')
        )
        pstr++;
    return (uni);
    }
    else if (*pmod != *pstr)
    return (OFFalse);
    return OFFalse;
}

/*******************
 *    Is the specified tag supported
 */

static int DB_TagSupported (DcmTagKey tag)
{
    int i;

    for (i = 0; i < NbFindAttr; i++)
    if (TbFindAttr[i]. tag == tag)
        return (OFTrue);

    return (OFFalse);

}


/*******************
 *    Get UID tag of a specified level
 */

static OFCondition DB_GetUIDTag (DB_LEVEL level, DcmTagKey *tag)
{
    int i;

    for (i = 0; i < NbFindAttr; i++)
    if ((TbFindAttr[i]. level == level) && (TbFindAttr[i]. keyAttr == UNIQUE_KEY))
        break;

    if (i < NbFindAttr) {
        *tag = TbFindAttr[i].tag;
        return (EC_Normal);
    }
    else
    return (DcmQRSqlDatabaseError);

}

/*******************
 *    Get tag level of a specified tag
 */

static OFCondition DB_GetTagLevel (DcmTagKey tag, DB_LEVEL *level)
{
    int i;

    for (i = 0; i < NbFindAttr; i++)
    if (TbFindAttr[i]. tag == tag)
        break;

    if (i < NbFindAttr) {
        *level = TbFindAttr[i]. level;
        return (EC_Normal);
    }
    else
    return (DcmQRSqlDatabaseError);
}

/*******************
 *    Get tag key attribute of a specified tag
 */

static OFCondition DB_GetTagKeyAttr (DcmTagKey tag, DB_KEY_TYPE *keyAttr)
{
    int i;

    for (i = 0; i < NbFindAttr; i++)
    if (TbFindAttr[i]. tag == tag)
        break;

    if (i < NbFindAttr) {
        *keyAttr = TbFindAttr[i]. keyAttr;
        return (EC_Normal);
    }
    else
    return (DcmQRSqlDatabaseError);
}

/*******************
 *    Get tag key attribute of a specified tag
 */

static OFCondition DB_GetTagKeyClass (DcmTagKey tag, DB_KEY_CLASS *keyAttr)
{
    int i;

    for (i = 0; i < NbFindAttr; i++)
    if (TbFindAttr[i]. tag == tag)
        break;

    if (i < NbFindAttr) {
        *keyAttr = TbFindAttr[i]. keyClass;
        return (EC_Normal);
    }
    else
    return (DcmQRSqlDatabaseError);
}


/*******************
 *    Remove spaces in a string
 */

static void DB_RemoveSpaces (char *string)
{
    char *pc1, *pc2;

    for (pc1 = pc2 = string; *pc2; pc2++) {
        if (*pc2 != ' ') {
            *pc1 = *pc2;
            pc1++;
        }
    }
    *pc1 = '\0';
}

/*******************
 *    Remove leading and trailing spaces in a string
 */

static void DB_RemoveEnclosingSpaces (char *string)
{
    char *pc1, *pc2;

    /** Find in pc2 the first non space character
    ** If not found, string is empty
    */

    for (pc2 = string; (*pc2 != '\0') && (*pc2 == ' '); pc2++);
    if (*pc2 == '\0') {
        string [0] = '\0';
        return;
    }

    /** Shift the string if necessary
     */

    if (pc2 != string) {
        for (pc1 = string; *pc2; pc1++, pc2++)
            *pc1 = *pc2;
        *pc1 = '\0';
    }

    /** Ship trailing spaces
     */

    for (pc2 = string + strlen (string) - 1; *pc2 == ' '; pc2--);
        pc2++;
    *pc2 = '\0';
}


/*******************
 *    Convert a date YYYYMMDD in a long
 */

static long DB_DateToLong (char *date)
{
    char year [5];
    char month[3];
    char day  [3];

    strncpy (year, date, 4);
    year [4] = '\0';
    strncpy (month, date + 4, 2);
    month [2] = '\0';
    strncpy (day, date + 6, 2);
    day [2] = '\0';

    return ((atol(year) * 10000) + (atol(month) * 100) + atol(day));
}


/*******************
 *    Convert a time in a double
 */

static double DB_TimeToDouble (char *thetime)
{
    char t [20];
    char tmp [4];

    double result = 0.;
    char *pc;

    /*** Get fractionnal part if exists
    **/

    strcpy (t, thetime);
    if ((pc = strchr (t, '.')) != NULL) {
        double f;

        *pc = '\0';
        for (pc++, f = 1.; (*pc) && (isdigit (*pc)); pc++) {
            f /= 10.;
            result += (*pc - '0') * f;
        }
    }

    /*** Add default values (mm ss) if necessary
    **/

    strcat (t, "0000");
    t [6] = '\0';

    /*** Get Hours, Minutes and Seconds
    **/

    strncpy (tmp, t, 2);
    tmp [3] = '\0';
    result += 3600. * OFStandard::atof(tmp);

    strncpy (tmp, t + 2, 2);
    tmp [3] = '\0';
    result += 60. * OFStandard::atof(tmp);

    strncpy (tmp, t + 4, 2);
    tmp [3] = '\0';
    result += OFStandard::atof(tmp);

    return result;
}



/***********************
 *    Duplicate a dicom element
 *    dst space is supposed provided by the caller
 */

static void DB_DuplicateElement (DB_SmallDcmElmt *src, DB_SmallDcmElmt *dst)
{

    bzero( (char*)dst, sizeof (DB_SmallDcmElmt));
    dst -> XTag = src -> XTag;
    dst -> ValueLength = src -> ValueLength;

    if (src -> ValueLength == 0)
        dst -> PValueField = NULL;
    else {
        dst -> PValueField = (char *)malloc ((int) src -> ValueLength+1);
        bzero(dst->PValueField, (size_t)(src->ValueLength+1));
        if (dst->PValueField != NULL) {
            memcpy (dst -> PValueField,  src -> PValueField,
                (size_t) src -> ValueLength);
        } else {
            DCMQRDB_ERROR("DB_DuplicateElement: out of memory");
        }
    }
}


/***********************
 *    Compare two ImagesofStudyArray elements
 */

extern "C" int DB_Compare(const void *ve1, const void *ve2)
{
    ImagesofStudyArray *e1 = (ImagesofStudyArray *)ve1;
    ImagesofStudyArray *e2 = (ImagesofStudyArray *)ve2;
    if ( e1 -> RecordedDate > e2 -> RecordedDate )
        return (1);
    else
    if ( e1 -> RecordedDate == e2 -> RecordedDate )
        return (0);
    else
        return (-1);

}


/* ==================================================================== */

DcmQueryRetrieveDatabaseHandle::~DcmQueryRetrieveDatabaseHandle()
{
}

/* ========================= FIND ========================= */

/************
**      Try to match Two Dates
**      The first one is the "model", the second one an element
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

int DcmQueryRetrieveSqlDatabaseHandle::matchDate (DB_SmallDcmElmt *mod, DB_SmallDcmElmt *elt)
{
    char date [DBC_MAXSTRING] ;
    char modl [DBC_MAXSTRING] ;

    /*** Get elt and model data in strings
    **/

    memcpy (date, elt->PValueField, (size_t)(elt->ValueLength)) ;
    date [elt->ValueLength] = '\0' ;
    DB_RemoveSpaces (date) ;

    memcpy (modl, mod->PValueField, (size_t)(mod->ValueLength)) ;
    modl [mod->ValueLength] = '\0' ;
    DB_RemoveSpaces (modl) ;

    /*** If no '-' in date
    *** return strict comparison result
    **/

    if (strchr (modl, '-') == NULL)
        return (strcmp (modl, date) == 0) ;

    /*** If first char is -
    **/

    if (modl [0] == '-') {
        return DB_DateToLong (date) <= DB_DateToLong (modl+1) ;
    }

    /*** If last char is -
    **/

    else if (modl [strlen (modl) - 1] == '-') {
        modl [strlen (modl) - 1] = '\0' ;
        return DB_DateToLong (date) >= DB_DateToLong (modl) ;
    }
    else {
        char *pc ;
        long d ;

        d = DB_DateToLong (date) ;
        pc = strchr (modl, '-') ;
        *pc = '\0' ;

        return (d >= DB_DateToLong (modl)) && (d <= DB_DateToLong (pc+1)) ;

    }
}

/************
**      Try to match Two Times
**      The first one is the "model", the second one an element
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

int DcmQueryRetrieveSqlDatabaseHandle::matchTime (DB_SmallDcmElmt *mod, DB_SmallDcmElmt *elt)
{
    char aTime [DBC_MAXSTRING] ;
    char modl [DBC_MAXSTRING] ;

    /*** Get elt and model data in strings
    **/

    memcpy (aTime, elt->PValueField, (size_t)(elt->ValueLength)) ;
    aTime [elt->ValueLength] = '\0' ;
    DB_RemoveSpaces (aTime) ;

    memcpy (modl, mod->PValueField, (size_t)(mod->ValueLength)) ;
    modl [mod->ValueLength] = '\0' ;
    DB_RemoveSpaces (modl) ;

    /*** If no '-' in time
    *** return strict comparison result
    **/

    if (strchr (modl, '-') == NULL)
        return (strcmp (modl, aTime) == 0) ;

    /*** If first char is -
    **/

    if (modl [0] == '-') {
        return DB_TimeToDouble (aTime) <= DB_TimeToDouble (modl+1) ;
    }

    /*** If last char is -
    **/

    else if (modl [strlen (modl) - 1] == '-') {
        modl [strlen (modl) - 1] = '\0' ;
        return DB_TimeToDouble (aTime) >= DB_TimeToDouble (modl) ;
    }
    else {
        char *pc ;
        double t ;

        t = DB_TimeToDouble (aTime) ;
        pc = strchr (modl, '-') ;
        *pc = '\0' ;

        return (t >= DB_TimeToDouble (modl)) && (t <= DB_TimeToDouble (pc+1)) ;

    }
}

/************
**      Try to match Two UID
**      The first one is the "model", the second one an element
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

int DcmQueryRetrieveSqlDatabaseHandle::matchUID (DB_SmallDcmElmt *mod, DB_SmallDcmElmt *elt)
{
    int match ;
    char *uid  ;
    char *modl  ;
    char *pc ;
    unsigned int length ;

    /*** Get elt and model data in strings
    **/

    uid = (char *) malloc ((size_t)(elt->ValueLength + 1)) ;
    if (uid == NULL) {
        return 0 ;
    }
    memcpy (uid, elt->PValueField, (size_t)(elt->ValueLength)) ;
    uid [elt->ValueLength] = '\0' ;

    modl = (char *) malloc ((size_t)(mod->ValueLength + 1)) ;
    if (modl == NULL) {
        free (uid) ;
        return 0 ;
    }
    memcpy (modl, mod->PValueField, (size_t)(mod->ValueLength)) ;
    modl [mod->ValueLength] = '\0' ;

    /*** If no '\' in model
    *** return strict comparison result
    **/

#ifdef STRICT_COMPARE
#else
    /*** Suppress Leading and Trailing spaces in
    *** model and string
    **/

    DB_RemoveEnclosingSpaces (uid) ;
    DB_RemoveEnclosingSpaces (modl) ;
#endif

    if (strchr (modl, '\\') == NULL) {
        match = (strcmp (modl, uid) == 0) ;
        free (uid) ;
        free (modl) ;
        return (match) ;
    }

    /*** UID List comparison.
    *** Match is successful if uid is found in model
    **/

    match = OFFalse ;
    for (pc = modl ; *pc ; ) {

        /*** Calculate the length to the next '\' sign (if any).
        *** Otherwise the length of pc is returned.
        **/
        length = strcspn(pc, "\\") ;

        if ((length == strlen(uid)) && (strncmp (pc, uid, length) == 0)) {
            match = OFTrue ;
            break ;
        }
        else {
            pc = strchr (pc, '\\') ;
            if (pc == NULL)
                break ;
            else
                pc++ ;
        }
    }

    free (uid) ;
    free (modl) ;
    return (match) ;

}

/************
**      Try to match Two Strings
**      The first one is the "model", the second one an element
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

int DcmQueryRetrieveSqlDatabaseHandle::matchStrings (DB_SmallDcmElmt *mod, DB_SmallDcmElmt *elt)
{
    int match ;
    char *string ;
    char *modl ;

    /*** Get elt and model data in strings
    **/

    string = (char *) malloc ((size_t)(elt->ValueLength + 1)) ;
    if (string == NULL) {
        return 0 ;
    }
    memcpy (string, elt->PValueField, (size_t)(elt->ValueLength)) ;
    string [elt->ValueLength] = '\0' ;

    modl = (char *) malloc ((size_t)(mod->ValueLength + 1)) ;
    if (modl == NULL) {
        free (string) ;
        return 0 ;
    }
    memcpy (modl, mod->PValueField, (size_t)(mod->ValueLength)) ;
    modl [mod->ValueLength] = '\0' ;

#ifdef STRICT_COMPARE
#else
    /*** Suppress Leading and Trailing spaces in
    *** model and string
    **/

    DB_RemoveEnclosingSpaces (string) ;
    DB_RemoveEnclosingSpaces (modl) ;
#endif

    /*** If no '*' and no '?' in model
    *** return strict comparison result
    **/

    if ((strchr (modl, '*') == NULL) && (strchr (modl, '?') == NULL))
        return (strcmp (modl, string) == 0) ;

    match = DB_StringUnify (modl, string) ;

    free (string) ;
    free (modl) ;
    return (match) ;

}

/************
**      Try to match Two Unknown elements
**      Strict comparaison is applied
**      The first one is the "model", the second one an element
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

int DcmQueryRetrieveSqlDatabaseHandle::matchOther (DB_SmallDcmElmt *mod, DB_SmallDcmElmt *elt)
{
    if (mod->ValueLength != elt->ValueLength)
        return OFFalse ;

    return (memcmp (mod->PValueField, elt->PValueField, (size_t)(elt->ValueLength)) == 0) ;
}

/************
**      Try to match Two DB_SmallDcmElmts
**      The first one is the "model", the second one an element
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

int DcmQueryRetrieveSqlDatabaseHandle::dbmatch (DB_SmallDcmElmt *mod, DB_SmallDcmElmt *elt)
{
    DB_KEY_CLASS keyClass = OTHER_CLASS;

    /*** If model length is 0
    *** Universal matching is applied : return always OFTrue
    **/

    if (mod->ValueLength == 0)
        return (OFTrue) ;

    /*** Get the key class of the element
    **/

    DB_GetTagKeyClass (elt->XTag, &keyClass) ;

    switch (keyClass) {

    case DATE_CLASS :
        return matchDate (mod, elt)  ;

    case TIME_CLASS :
        return matchTime (mod, elt)  ;

    case UID_CLASS :
        return matchUID  (mod, elt) ;

    case STRING_CLASS :
        return matchStrings (mod, elt) ;

    case OTHER_CLASS :
        return matchOther (mod, elt) ;

    }
    return OFFalse;
}

/************
**      Create the response list in specified handle,
**      using informations found in an SQL record.
**      Old response list is supposed freed
**/

void DcmQueryRetrieveSqlDatabaseHandle::makeSqlResponseList (
                DB_Private_Handle       *phandle
                )
{
    unsigned int colnum;
    DB_ElementList *pRequestList = NULL;
    DB_ElementList *plist = NULL;
    DB_ElementList *last = NULL;

    std::string s = "";
    phandle->findResponseList = NULL ;
    /*** For each element in Request identifier
    **/
    DCMQRDB_INFO("En makeSqlResponseList");
    for (pRequestList = phandle->findRequestList ; pRequestList ; pRequestList = pRequestList->next) {
        /*** se compara el tag actual con la lista
             de campos de la consulta SQL
        */
        s = "";
        DcmTag *lDcmTag = new DcmTag(pRequestList->elem.XTag);
        std::string lTag;
        int lComp;

        for (colnum=0; colnum < PQnfields(internalResult); ++colnum)
        {
           lTag = lDcmTag->getTagName();
           lComp = lTag.compare("PatientName"); 
           if(lComp == 0)
           {
             lTag = "PatientName";
           }
           lComp = lTag.compare("PatientBirthDate"); 
           if(lComp == 0)
           {
             lTag = "PatientBirthDate";
           }
           lComp = lTag.compare("PatientSex"); 
           if(lComp == 0)
           {
             lTag = "PatientSex";
           }
           lComp = lTag.compare("PatientAge"); 
           if(lComp == 0)
           {
             lTag = "PatientAge";
             DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
           }
           lComp = lTag.compare( "ReferringPhysicianName"); 
           if(lComp == 0)
           {
             lTag = "ReferringPhysiciansName";
             DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
           }

           if(strcasecmp(PQfname(internalResult, colnum), lTag.c_str()) == 0)
           {
              s = PQgetvalue(internalResult, queryPos, colnum);
              break;
           }
        }

        // plist es el tag resultado
        plist = (DB_ElementList *) malloc (sizeof (DB_ElementList)) ;
        if (plist == NULL) {
            DCMQRDB_ERROR("makeSqlResponseList: out of memory");
            return;
        }
        plist->elem.XTag.set(pRequestList->elem.XTag);
        plist->elem.ValueLength = 0;

        /*** ATENCION!!! si se pide un tag cuya keyClass
             no está implementada en el cuerpo del switch
             da un A.V., por este motivo deben implementarse
             TODAS las keyClass.
        ***/
        plist->elem.ValueLength = s.size();
        plist->elem.PValueField = (char*)malloc((size_t)(plist->elem.ValueLength+1));
        strcpy(plist->elem.PValueField, s.c_str());

        plist->next = NULL;
        if (phandle->findResponseList == NULL) {
            phandle->findResponseList = last = plist ;
        }
        else {
            last->next = plist ;
            last = plist ;
        }
    }
}


/************
**      Create the response list in specified handle,
**      using informations found in an index record.
**      Old response list is supposed freed
**/

void DcmQueryRetrieveSqlDatabaseHandle::makeResponseList (
                DB_Private_Handle       *phandle,
                IdxRecord               *idxRec
                )
{
    DB_ElementList *pRequestList = NULL;
    DB_ElementList *plist = NULL;
    DB_ElementList *last = NULL;

    phandle->findResponseList = NULL ;

    /*** For each element in Request identifier
    **/

    DCMQRDB_INFO("En makeResponseList");

    for (pRequestList = phandle->findRequestList ; pRequestList ; pRequestList = pRequestList->next) {

        plist = (DB_ElementList *) malloc (sizeof (DB_ElementList)) ;
        if (plist == NULL) {
            DCMQRDB_ERROR("makeResponseList: out of memory");
            return;
        }
        plist->next = NULL;

        plist->elem.XTag.set(pRequestList->elem.XTag);

        if (phandle->findResponseList == NULL) {
            phandle->findResponseList = last = plist ;
        }
        else {
            last->next = plist ;
            last = plist ;
        }
    }
}

/************
**      Test a Find Request List
**      Returns EC_Normal if ok, else returns DcmQRSqlDatabaseError
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::testFindRequestList (
                DB_ElementList  *findRequestList,
                DB_LEVEL        queryLevel,
                DB_LEVEL        infLevel,
                DB_LEVEL        lowestLevel
                )
{
    DB_ElementList *plist ;
    DB_LEVEL    XTagLevel ;
    DB_KEY_TYPE XTagType ;
    int level ;

    /**** Query level must be at least the infLevel
    ***/

    if (queryLevel < infLevel) {
        DCMQRDB_INFO("Level incompatible with Information Model (level " << queryLevel << ")");
        return DcmQRSqlDatabaseError ;
    }

    if (queryLevel > lowestLevel) {
        DCMQRDB_DEBUG("Level incompatible with Information Model (level " << queryLevel << ")");
        return DcmQRSqlDatabaseError ;
    }

    for (level = PATIENT_LEVEL ; level <= IMAGE_LEVEL ; level++) {

        /**** Manage exception due to StudyRoot Information Model :
        **** In this information model, queries may include Patient attributes
        **** but only if they are made at the study level
        ***/

        if ((level == PATIENT_LEVEL) && (infLevel == STUDY_LEVEL)) {
            /** In Study Root Information Model, accept only Patient Tags
            ** if the Query Level is the Study level
            */

            int atLeastOneKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                atLeastOneKeyFound = OFTrue ;
            }
            if (atLeastOneKeyFound && (queryLevel != STUDY_LEVEL)) {
                DCMQRDB_DEBUG("Key found in Study Root Information Model (level " << level << ")");
                return DcmQRSqlDatabaseError ;
            }
        }

        /**** If current level is above the QueryLevel
        ***/

        else if (level < queryLevel) {

            /** For this level, only unique keys are allowed
            ** Parse the request list elements reffering to
            ** this level.
            ** Check that only unique key attr are provided
            */

            int uniqueKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                DB_GetTagKeyAttr (plist->elem. XTag, &XTagType) ;
                if (XTagType != UNIQUE_KEY) {
                    DCMQRDB_DEBUG("Non Unique Key found (level " << level << ")");
                    return DcmQRSqlDatabaseError ;
                }
                else if (uniqueKeyFound) {
                    DCMQRDB_DEBUG("More than one Unique Key found (level " << level << ")");
                    return DcmQRSqlDatabaseError ;
                }
                else
                    uniqueKeyFound = OFTrue ;
            }
        }

        /**** If current level is the QueryLevel
        ***/

        else if (level == queryLevel) {

            /** For this level, all keys are allowed
            ** Parse the request list elements reffering to
            ** this level.
            ** Check that at least one key is provided
            */

            int atLeastOneKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                atLeastOneKeyFound = OFTrue ;
            }
            if (! atLeastOneKeyFound) {
                DCMQRDB_DEBUG("No Key found at query level (level " << level << ")");
                return DcmQRSqlDatabaseError ;
            }
        }

        /**** If current level beyond the QueryLevel
        ***/

        else if (level > queryLevel) {

            /** For this level, no key is allowed
            ** Parse the request list elements reffering to
            ** this level.
            ** Check that no key is provided
            */

            int atLeastOneKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                atLeastOneKeyFound = OFTrue ;
            }
            if (atLeastOneKeyFound) {
                DCMQRDB_DEBUG("Key found beyond query level (level " << level << ")");
                return DcmQRSqlDatabaseError ;
            }
        }

    }
    return EC_Normal ;
}


/************
**      Hierarchical Search Algorithm
**      Returns OFTrue if matching is ok, else returns OFFalse
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::hierarchicalCompare (
                DB_Private_Handle       *phandle,
                IdxRecord               *idxRec,
                DB_LEVEL                level,
                DB_LEVEL                infLevel,
                int                     *match)
{
    int                 i ;
    DcmTagKey   XTag ;
    DB_ElementList *plist ;
    DB_LEVEL    XTagLevel ;

    /**** If current level is above the QueryLevel
    ***/

    if (level < phandle->queryLevel) {

        /** Get UID Tag for current level
         */

        DB_GetUIDTag (level, &XTag) ;

        /** Find Element with this XTag in Identifier list
         */

        for (plist = phandle->findRequestList ; plist ; plist = plist->next)
            if (plist->elem. XTag == XTag)
                break ;

        /** Element not found
         */

        if (plist == NULL) {
            *match = OFFalse ;
            DCMQRDB_WARN("hierarchicalCompare : No UID Key found at level " << (int) level);
            return DcmQRSqlDatabaseError ;
        }

        /** Find element with the same XTag in index record
         */

        for (i = 0 ; i < NBPARAMETERS ; i++)
            if (idxRec->param [i]. XTag == XTag)
                break ;

        /** Compare with Single value matching
        ** If Match fails, return OFFalse
        */

        if (! dbmatch (&(plist->elem), &idxRec->param[i])) {
            *match = OFFalse ;
            return EC_Normal ;
        }

        /** Match succeeded.
        ** Try at next level
        */

        return hierarchicalCompare (phandle, idxRec, (DB_LEVEL)(level + 1), infLevel, match) ;
    }

    /**** If current level is the QueryLevel
    ***/

    else if (level == phandle->queryLevel) {

        /*** For each element in Identifier list
        **/

        for (plist = phandle->findRequestList ; plist ; plist = plist->next) {

            /** Get the Tag level of this element
             */

            DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;

            /** If we are in the Study Root Information Model exception
            ** we must accept patients keys at the study level
            */

            if (  (XTagLevel == PATIENT_LEVEL)
                  && (phandle->queryLevel == STUDY_LEVEL)
                  && (infLevel == STUDY_LEVEL)
                ) ;

            /** In other cases, only keys at the current level are
            ** taken into account. So skip this element.
            */

            else if (XTagLevel != level)
                continue ;

            /** Find element with the same XTag in index record
             */

            for (i = 0 ; i < NBPARAMETERS ; i++)
                if (idxRec->param [i]. XTag == plist->elem. XTag)
                    break ;

            /** Compare with appropriate Matching.
            ** If Match fails, return OFFalse
            */


            if (! dbmatch (&(plist->elem), &idxRec->param[i])) {
                *match = OFFalse ;
                return EC_Normal ;
            }
        }

        /*** If we are here, all matches succeeded at the current level.
        *** Perhaps check that we have tried at least one match ??
        **/

        *match = OFTrue ;
        return EC_Normal ;

    }
    return DcmQRSqlDatabaseError;
}


/********************
**      Genera el filtro para fechas
**      cuando el valor del campo StudyDate
**      es del tipo yyyymmdd-yyyymmdd
**      lo convierte en "between yyyymmdd and yyyymmdd
**/

std::string DcmQueryRetrieveSqlDatabaseHandle::getDateRange(
                const std::string&   StudyDate)
{
  unsigned int lPos = StudyDate.find("-");

  std::string lFrom = StudyDate.substr(0, lPos);
  std::string lTo = StudyDate.substr(lPos + 1);
  if(lTo == "")
   lTo = lFrom;

  if(lPos != std::string::npos) {
    std::string lRes = "between '";
    lRes = lRes + lFrom;
    lRes = lRes + "' and '";
    lRes = lRes + lTo + "'";
    return lRes;
  }
  else
    return " = '" + StudyDate + "'";
}


/********************
**      Crea el SQL para hacer CFind en la base de datos
**/

std::string DcmQueryRetrieveSqlDatabaseHandle::getCFindSql(
                DB_ElementList  *findRequestList,
                DB_LEVEL        queryLevel)
{
    std::string         lWhere = "";
    std::string         lSql = "";
    std::string         lTable = "";
    std::string         lGroupBy = "";
    std::string         lTag = "";
    std::string         lValue =  "";

    DB_ElementList      *plist = NULL;

    switch (queryLevel) {
    case PATIENT_LEVEL :
        lTable = PATIENT_LEVEL_STRING ;
        break ;
    case STUDY_LEVEL :
        lTable = STUDY_LEVEL_STRING ;
        break ;
    case SERIE_LEVEL :
        lTable = SERIE_LEVEL_STRING ;
        break ;
    case IMAGE_LEVEL :
        lTable = IMAGE_LEVEL_STRING ;
        break ;
    }

    lSql = "select ";
    int lComp;
    /* Se genera la lista de campos */
    for (plist = findRequestList ; plist ; plist = plist->next) {
        DcmTag *lDcmTag = new DcmTag(plist->elem.XTag);
        lTag = lDcmTag->getTagName();

        lComp = lTag.compare("PatientName"); 
        if(lComp == 0)
        {
          lTag = "PatientName";
          DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
        }
        lComp = lTag.compare("PatientBirthDate"); 
        if(lComp == 0)
        {
          lTag = "PatientBirthDate";
          DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
        }
        lComp = lTag.compare("PatientSex"); 
        if(lComp == 0)
        {
          lTag = "PatientSex";
          DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
        }
        lComp = lTag.compare("PatientAge"); 
        if(lComp == 0)
        {
          lTag = "PatientAge";
          DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
        }
        lComp = lTag.compare( "ReferringPhysicianName"); 
        if(lComp == 0)
        {
          lTag = "ReferringPhysiciansName";
          DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
        }


        if(lTag == "NumberOfSeriesRelatedInstances") {
          lTag = "count(*) as " + lTag;
        }

        if(lTag == "NumberOfStudyRelatedSeries") {
          continue;
        }

        if(lTag == "NumberOfStudyRelatedInstances") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedStudies") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedSeries") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedInstances") {
          continue;
        }

        if(lTag == "PatientSize") {
          continue;
        }

        if(lTag == "PatientWeight") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedInstances") {
          continue;
        }

        lSql = lSql + lTag + ",";
    }

    /* Eliminamos la última coma */
    lSql.erase(lSql.length() - 1, 1);

    if(lTable == "STUDY")
    {
      lTable = "STUDY s join patient p on p.idpatient=s.idpatient ";
      lTable = lTable + "join series se on se.idstudy=s.idstudy ";
      lTable = lTable + "join image i on i.idseries = se.idseries ";
    }
    else
    if(lTable == "SERIES") {
      lTable = "SERIES se join study s on se.idstudy=s.idstudy ";
      lTable = lTable + "join image i on i.idseries = se.idseries ";
      lTable = lTable + "join patient p on p.idpatient = s.idpatient ";
    }
    else
    if(lTable == "IMAGE")
    {
      lTable = "IMAGE im join series se on se.idseries=im.idseries ";
      lTable = lTable + "join study s on se.idstudy=s.idstudy ";
      lTable = lTable + "join patient p on p.idpatient = s.idpatient ";
    }

    lSql = lSql + " from " + lTable;

    /* Se genera el Where */
    for (plist = findRequestList ; plist ; plist = plist->next) {
        DcmTag *lDcmTag = new DcmTag(plist->elem.XTag);
        lTag = lDcmTag->getTagName();
    
        lComp = lTag.compare("PatientName"); 
        if(lComp == 0)
        {
          lTag = "PatientName";
        }
        lComp = lTag.compare("PatientBirthDate"); 
        if(lComp == 0)
        {
          lTag = "PatientBirthDate";
        }
        lComp = lTag.compare("PatientSex"); 
        if(lComp == 0)
        {
          lTag = "PatientSex";
        }
        lComp = lTag.compare("PatientAge"); 
        if(lComp == 0)
        {
          lTag = "PatientAge";
        }
        lComp = lTag.compare( "ReferringPhysicianName"); 
        if(lComp == 0)
        {
          lTag = "ReferringPhysiciansName";
        }

        /* Si el tag no tiene valor lo ignoramos */
        if(plist->elem.PValueField == NULL)
          continue;
        lValue = plist->elem.PValueField;
        if(lWhere == "")
          lWhere = " where ";

        if(lValue.find("*") != std::string::npos) {
          lValue = lValue.substr(0, lValue.find("*"));
          lWhere = lWhere + lTag + " like '" + lValue + "%' and ";
        }
        else
        if(lTag == "StudyDate") {
          lWhere = lWhere + "StudyDate " + getDateRange(lValue) + " and ";
        }
        else
          lWhere = lWhere + lTag + "='" + lValue + "' and ";
    }

    /* Eliminamos el " and " */
    if(lWhere != "")
      lWhere.erase(lWhere.length() - 5, 5);

    /* Se crea el Group by  */
    if(queryLevel == SERIE_LEVEL) {
      lGroupBy = " Group By se.idseries, ";
      // Generamos la lista de campos que deben aparecer
      // también en el Group By
      for (plist = findRequestList ; plist ; plist = plist->next) {
          DcmTag *lDcmTag = new DcmTag(plist->elem.XTag);
          lTag = lDcmTag->getTagName();

          lComp = lTag.compare("PatientName"); 
          if(lComp == 0)
          {
            lTag = "PatientName";
            DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
          }
          lComp = lTag.compare("PatientBirthDate"); 
          if(lComp == 0)
          {
            lTag = "PatientBirthDate";
            DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
          }
          lComp = lTag.compare("PatientSex"); 
          if(lComp == 0)
          {
            lTag = "PatientSex";
            DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
          }
          lComp = lTag.compare("PatientAge"); 
          if(lComp == 0)
          {
            lTag = "PatientAge";
            DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
          }
          lComp = lTag.compare( "ReferringPhysicianName"); 
          if(lComp == 0)
          {
            lTag = "ReferringPhysiciansName";
            DCMQRDB_INFO("Nuevo Tag: " << lTag.c_str());
          }


          if(lTag == "NumberOfSeriesRelatedInstances") {
            continue;
          }
          if(lTag == "NumberOfStudyRelatedInstances") {
            continue;
          }

          lGroupBy = lGroupBy + lTag + ",";
      }
      /* Eliminamos la última coma */
      lGroupBy.erase(lGroupBy.length() - 1, 1);
    }
    else
    if(queryLevel == STUDY_LEVEL) {
      lGroupBy = " Group By se.idstudy, ";
      // Generamos la lista de campos que deben aparecer
      // también en el Group By
      for (plist = findRequestList ; plist ; plist = plist->next) {
          DcmTag *lDcmTag = new DcmTag(plist->elem.XTag);
          lTag = lDcmTag->getTagName();

          lComp = lTag.compare("PatientName"); 
          if(lComp == 0)
          {
            lTag = "PatientName";
          }
          lComp = lTag.compare("PatientBirthDate"); 
          if(lComp == 0)
          {
            lTag = "PatientBirthDate";
          }
          lComp = lTag.compare("PatientSex"); 
          if(lComp == 0)
          {
            lTag = "PatientSex";
          }
          lComp = lTag.compare("PatientAge"); 
          if(lComp == 0)
          {
            lTag = "PatientAge";
          }
          lComp = lTag.compare( "ReferringPhysicianName"); 
          if(lComp == 0)
          {
            lTag = "ReferringPhysiciansName";
          }

        if(lTag == "NumberOfStudyRelatedSeries") {
          continue;
        }

        if(lTag == "NumberOfStudyRelatedInstances") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedStudies") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedSeries") {
          continue;
        }

        if(lTag == "NumberOfPatientRelatedInstances") {
          continue;
        }

          lGroupBy = lGroupBy + lTag + ",";
      }
      /* Eliminamos la última coma */
      lGroupBy.erase(lGroupBy.length() - 1, 1);
    }

    /* Se concatena el sql y el where */
    lSql = lSql + lWhere + lGroupBy;

    DCMQRDB_INFO(lSql);
    return lSql;
}

/********************
**      Crea el SQL para hacer CMove en la base de datos
**/

std::string DcmQueryRetrieveSqlDatabaseHandle::getCMoveSql(
                DB_ElementList  *findRequestList,
                DB_LEVEL        queryLevel)
{
    std::string         lWhere = "";
    std::string         lSql = "";
    std::string         lTable = "";
    std::string         lTag = "";
    std::string         lValue =  "";

    DB_ElementList      *plist = NULL;

    switch (queryLevel) {
    case PATIENT_LEVEL :
        lTable = PATIENT_LEVEL_STRING ;
        break ;
    case STUDY_LEVEL :
        lTable = STUDY_LEVEL_STRING ;
        break ;
    case SERIE_LEVEL :
        lTable = SERIE_LEVEL_STRING ;
        break ;
    case IMAGE_LEVEL :
        lTable = IMAGE_LEVEL_STRING ;
        break ;
    }

    lSql = "select SOPClassUID, SOPInstanceUID, ImagePath ";
    lSql = lSql + "from Series se ";
    lSql = lSql + "join study s on se.idstudy = s.idstudy ";
    lSql = lSql + "join image i on i.idseries = se.idseries ";
    lSql = lSql + "join patient p on p.idpatient = s.idpatient ";

    /* Se genera el Where */
    for (plist = findRequestList ; plist ; plist = plist->next) {
        DcmTag *lDcmTag = new DcmTag(plist->elem.XTag);
        lTag = lDcmTag->getTagName();

        /* Si el tag no tiene valor lo ignoramos */
        if(plist->elem.PValueField == NULL)
          continue;
        lValue = plist->elem.PValueField;
        if(lWhere == "")
          lWhere = " where ";

        if(lValue.find("*") != std::string::npos) {
          lValue = lValue.substr(0, lValue.find("*"));
          lWhere = lWhere + lTag + " like '" + lValue + "%' and ";
        }
        else
        if(lTag == "StudyDate") {
          lWhere = lWhere + "StudyDate " + getDateRange(lValue) + " and ";
        }
        else
          lWhere = lWhere + lTag + "='" + lValue + "' and ";
    }

    /* Eliminamos el " and " */
    if(lWhere != "")
      lWhere.erase(lWhere.length() - 5, 5);

    /* Se concatena el sql y el where */
    lSql = lSql + lWhere;

    DCMQRDB_INFO(lSql);
    return lSql;
}


/********************
**      Connect to db
**/
PGconn * DcmQueryRetrieveSqlDatabaseHandle::connectToDb()
{
    std::string lConn;
    lConn = connectionString_;

    return PQconnectdb(lConn.c_str());
}

/********************
**      Disconnect from db
**/
void DcmQueryRetrieveSqlDatabaseHandle::disconnectFromDb(PGconn * connection)
{
    PQfinish(connection);
}

/********************
**      Start find in Database
**/

OFCondition DcmQueryRetrieveSqlDatabaseHandle::startFindRequest(
                const char      *SOPClassUID,
                DcmDataset      *findRequestIdentifiers,
                DcmQueryRetrieveDatabaseStatus  *status)
{

    DB_SmallDcmElmt     elem ;
    DB_ElementList      *plist = NULL;
    DB_ElementList      *last = NULL;
    std::string         lSql = "";

    int                 MatchFound ;
    IdxRecord           idxRec ;
    DB_LEVEL            qLevel = PATIENT_LEVEL; // highest legal level for a query in the current model
    DB_LEVEL            lLevel = IMAGE_LEVEL;   // lowest legal level for a query in the current model

    OFCondition         cond = EC_Normal;
    OFBool qrLevelFound = OFFalse;

    /**** Is SOPClassUID supported ?
    ***/

    if (strcmp( SOPClassUID, UID_FINDPatientRootQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = PATIENT_ROOT ;
    else if (strcmp( SOPClassUID, UID_FINDStudyRootQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = STUDY_ROOT ;
#ifndef NO_PATIENTSTUDYONLY_SUPPORT
    else if (strcmp( SOPClassUID, UID_RETIRED_FINDPatientStudyOnlyQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = PATIENT_STUDY ;
#endif
    else {
        status->setStatus(STATUS_FIND_Refused_SOPClassNotSupported);
        return (DcmQRSqlDatabaseError) ;
    }


    /**** Parse Identifiers in the Dicom Object
    **** Find Query Level and contruct a list
    **** of query identifiers
    ***/

    handle_->findRequestList = NULL ;

    int elemCount = (int)(findRequestIdentifiers->card());
    for (int elemIndex=0; elemIndex<elemCount; elemIndex++) {

        DcmElement* dcelem = findRequestIdentifiers->getElement(elemIndex);

        elem.XTag = dcelem->getTag().getXTag();
        if(!DB_TagSupported(elem.XTag)){
          DCMQRDB_INFO("Tag no soportado -->" << elem.XTag);
        }
        if (elem.XTag == DCM_QueryRetrieveLevel || DB_TagSupported(elem.XTag)) {
            elem.ValueLength = dcelem->getLength();
            if (elem.ValueLength == 0) {
                elem.PValueField = NULL ;
            } else if ((elem.PValueField = (char*)malloc((size_t)(elem.ValueLength+1))) == NULL) {
                status->setStatus(STATUS_FIND_Refused_OutOfResources);
                return (DcmQRSqlDatabaseError) ;
            } else {
                /* only char string type tags are supported at the moment */
                char *s = NULL;
                dcelem->getString(s);
                strcpy(elem.PValueField, s);
            }
            /** If element is the Query Level, store it in handle
             */

            if (elem.XTag == DCM_QueryRetrieveLevel) {
                char *pc ;
                char level [50] ;

                strncpy(level, (char*)elem.PValueField,
                        (elem.ValueLength<50)? (size_t)(elem.ValueLength) : 49) ;

                /*** Skip this two lines if you want strict comparison
                **/

                for (pc = level ; *pc ; pc++)
                    *pc = ((*pc >= 'a') && (*pc <= 'z')) ? 'A' - 'a' + *pc : *pc ;

                if (strncmp (level, PATIENT_LEVEL_STRING,
                             strlen (PATIENT_LEVEL_STRING)) == 0)
                    handle_->queryLevel = PATIENT_LEVEL ;
                else if (strncmp (level, STUDY_LEVEL_STRING,
                                  strlen (STUDY_LEVEL_STRING)) == 0)
                    handle_->queryLevel = STUDY_LEVEL ;
                else if (strncmp (level, SERIE_LEVEL_STRING,
                                  strlen (SERIE_LEVEL_STRING)) == 0)
                    handle_->queryLevel = SERIE_LEVEL ;
                else if (strncmp (level, IMAGE_LEVEL_STRING,
                                  strlen (IMAGE_LEVEL_STRING)) == 0)
                    handle_->queryLevel = IMAGE_LEVEL ;
                else {
                    if (elem.PValueField)
                        free (elem.PValueField) ;
#ifdef DEBUG
                    DCMQRDB_DEBUG("DB_startFindRequest () : Illegal query level (" << level << ")");
#endif
                    status->setStatus(STATUS_FIND_Failed_UnableToProcess);
                    return (DcmQRSqlDatabaseError) ;
                }
                qrLevelFound = OFTrue;
            } else {
                /** Else it is a query identifier.
                ** Append it to our RequestList if it is supported
                */
                if (DB_TagSupported (elem. XTag)) {

                    plist = (DB_ElementList *) malloc (sizeof (DB_ElementList)) ;
                    if (plist == NULL) {
                        status->setStatus(STATUS_FIND_Refused_OutOfResources);
                        return (DcmQRSqlDatabaseError) ;
                    }
                    plist->next = NULL ;
                    DB_DuplicateElement (&elem, &(plist->elem)) ;
                    if (handle_->findRequestList == NULL) {
                        handle_->findRequestList = last = plist ;
                    } else {
                        last->next = plist ;
                        last = plist ;
                    }
                }
            }

            if ( elem. PValueField ) {
                free (elem. PValueField) ;
            }
        }
    }

    if (!qrLevelFound) {
        /* The Query/Retrieve Level is missing */
        status->setStatus(STATUS_FIND_Failed_IdentifierDoesNotMatchSOPClass);
        DCMQRDB_WARN("DB_startFindRequest(): missing Query/Retrieve Level");
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        return (DcmQRSqlDatabaseError) ;
    }

    switch (handle_->rootLevel)
    {
      case PATIENT_ROOT :
        qLevel = PATIENT_LEVEL ;
        lLevel = IMAGE_LEVEL ;
        break ;
      case STUDY_ROOT :
        qLevel = STUDY_LEVEL ;
        lLevel = IMAGE_LEVEL ;
        break ;
      case PATIENT_STUDY:
        qLevel = PATIENT_LEVEL ;
        lLevel = STUDY_LEVEL ;
        break ;
    }

    /**** Test the consistency of the request list
    ***/

    if (doCheckFindIdentifier) {
        cond = testFindRequestList (handle_->findRequestList, handle_->queryLevel, qLevel, lLevel) ;
        if (cond != EC_Normal) {
            handle_->idxCounter = -1 ;
            DB_FreeElementList (handle_->findRequestList) ;
            handle_->findRequestList = NULL ;
#ifdef DEBUG
            DCMQRDB_DEBUG("DB_startFindRequest () : STATUS_FIND_Failed_IdentifierDoesNotMatchSOPClass - Invalid RequestList");
#endif
            status->setStatus(STATUS_FIND_Failed_IdentifierDoesNotMatchSOPClass);
            return (cond) ;
        }
    }

    /**** Se obtiene el SQL para realizar la búsqueda
    ***/
    lSql = getCFindSql(handle_->findRequestList, handle_->queryLevel);
    DCMQRDB_INFO("After getCFindSql");

    std::string lConn;
    lConn = connectionString_;

    PGconn *dbconn = PQconnectdb(lConn.c_str());
    if(PQstatus(dbconn) == CONNECTION_OK) 
    {
      PQexec(dbconn, CLIENT_ENCODING);

      internalResult = PQexec(dbconn, lSql.c_str());
      queryPos = 0;

      if (PQresultStatus(internalResult) != PGRES_TUPLES_OK)
      {
        PQclear(internalResult);
        DCMQRDB_ERROR("Error." << PQerrorMessage(dbconn));
        PQfinish(dbconn);
        return(DcmQRSqlDatabaseError); 
      };

      if(PQntuples(internalResult) == 0)
      {
        DCMQRDB_DEBUG("No matches found.");
        MatchFound = OFFalse;
        cond = EC_Normal;
      } else {
        MatchFound = OFTrue;
      }
    }
    else
    {
      DCMQRDB_ERROR(PQerrorMessage(dbconn));
      PQfinish(dbconn);
      MatchFound = OFFalse ;
      return (DcmQRSqlDatabaseError) ;
    }
    PQfinish(dbconn);


    /**** If an error occured in Matching function
    ****    return a failed status
    ***/

    if (cond != EC_Normal) {
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        status->setStatus(STATUS_FIND_Failed_UnableToProcess);

        return (cond) ;
    }


    /**** If a matching image has been found,
    ****         add index record to UID found list
    ****    prepare Response List in handle
    ****    return status is pending
    ***/

    if (MatchFound) {
        DCMQRDB_INFO("MatchFound");
        DB_UIDAddFound (handle_, &idxRec) ;
        makeResponseList (handle_, &idxRec) ;
        status->setStatus(STATUS_Pending);
        return (EC_Normal) ;
    }

    /**** else no matching image has been found,
    ****    free query identifiers list
    ****    status is success
    ***/

    else {
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        status->setStatus(STATUS_Success);
        return (EC_Normal) ;
    }

}

/********************
**      Get next find response in Database
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::nextFindResponse (
                DcmDataset      **findResponseIdentifiers,
                DcmQueryRetrieveDatabaseStatus  *status)
{
    DB_ElementList      *plist = NULL;
    IdxRecord           idxRec ;
    DB_LEVEL            qLevel = PATIENT_LEVEL;
    const char          *queryLevelString = NULL;
    OFCondition         cond = EC_Normal;

    if (handle_->findResponseList == NULL) {
        *findResponseIdentifiers = NULL ;
        status->setStatus(STATUS_Success);
        return (EC_Normal) ;
    }
    makeSqlResponseList(handle_) ;

    /***** Create the response (findResponseIdentifiers) using
    ***** the last find done and saved in handle findResponseList
    ****/
    *findResponseIdentifiers = new DcmDataset ;
    if ( *findResponseIdentifiers != NULL ) {
        for ( plist = handle_->findResponseList ; plist != NULL ; plist = plist->next ) {
            DcmTag t(plist->elem.XTag);
            DcmElement *dce = newDicomElement(t);
            if (dce == NULL) {
                status->setStatus(STATUS_FIND_Refused_OutOfResources);
                return DcmQRSqlDatabaseError;
            }
            if (plist->elem.PValueField != NULL &&
                strlen(plist->elem.PValueField) > 0) {
                OFCondition ec = dce->putString(plist->elem.PValueField);
                if (ec != EC_Normal) {
                    DCMQRDB_WARN("dbfind: DB_nextFindResponse: cannot put()");
                    status->setStatus(STATUS_FIND_Failed_UnableToProcess);
                    return DcmQRSqlDatabaseError;
                }
            }
            OFCondition ec = (*findResponseIdentifiers)->insert(dce, OFTrue);
            if (ec != EC_Normal) {
                DCMQRDB_WARN("dbfind: DB_nextFindResponse: cannot insert()");
                status->setStatus(STATUS_FIND_Failed_UnableToProcess);
                return DcmQRSqlDatabaseError;
            }
        }

        switch (handle_->queryLevel) {
        case PATIENT_LEVEL :
            queryLevelString = PATIENT_LEVEL_STRING ;
            break ;
        case STUDY_LEVEL :
            queryLevelString = STUDY_LEVEL_STRING ;
            break ;
        case SERIE_LEVEL :
            queryLevelString = SERIE_LEVEL_STRING ;
            break ;
        case IMAGE_LEVEL :
            queryLevelString = IMAGE_LEVEL_STRING ;
            break ;
        }
        DU_putStringDOElement(*findResponseIdentifiers,
                              DCM_QueryRetrieveLevel, queryLevelString);
    }
    else {
        return (DcmQRSqlDatabaseError) ;
    }

    switch (handle_->rootLevel) {
    case PATIENT_ROOT : qLevel = PATIENT_LEVEL ;        break ;
    case STUDY_ROOT :   qLevel = STUDY_LEVEL ;          break ;
    case PATIENT_STUDY: qLevel = PATIENT_LEVEL ;        break ;
    }


    /***** Si se llegó al final de la lista se libera la respuesta
    ****/
    if(queryPos == PQntuples(internalResult) - 1){
      DB_FreeElementList (handle_->findResponseList) ;
      handle_->findResponseList = NULL ;
    }
    else
    {
      queryPos = queryPos + 1;
    }

    /***** ... and find the next one
    ****/
    OFBool MatchFound = OFTrue ;
    cond = EC_Normal ;


    /**** If an error occured in Matching function
    ****    return status is pending
    ***/
    if (cond != EC_Normal) {
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        status->setStatus(STATUS_FIND_Failed_UnableToProcess);
        return (cond) ;
    }

    /**** If a matching image has been found
    ****    add index records UIDs in found UID list
    ****    prepare Response List in handle
    ***/

    /* Se envia el  */
    status->setStatus(STATUS_Pending);
    return (EC_Normal) ;
}

/********************
**      Cancel find request
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::cancelFindRequest (DcmQueryRetrieveDatabaseStatus *status)
{
    handle_->idxCounter = -1 ;
    DB_FreeElementList (handle_->findRequestList) ;
    handle_->findRequestList = NULL ;
    DB_FreeElementList (handle_->findResponseList) ;
    handle_->findResponseList = NULL ;
    DB_FreeUidList (handle_->uidList) ;
    handle_->uidList = NULL ;

    status->setStatus(STATUS_FIND_Cancel_MatchingTerminatedDueToCancelRequest);

    return (EC_Normal) ;
}

/* ========================= MOVE ========================= */

/************
 *      Test a Move Request List
 *      Returns EC_Normal if ok, else returns DcmQRSqlDatabaseError
 */

OFCondition DcmQueryRetrieveSqlDatabaseHandle::testMoveRequestList (
                DB_ElementList  *findRequestList,
                DB_LEVEL        queryLevel,
                DB_LEVEL        infLevel,
                DB_LEVEL        lowestLevel
                )
{
    DB_ElementList *plist ;
    DB_LEVEL    XTagLevel ;
    DB_KEY_TYPE         XTagType ;
    int level ;

    /**** Query level must be at least the infLevel
    ***/

    if (queryLevel < infLevel) {
        DCMQRDB_DEBUG("Level incompatible with Information Model (level " << (int)queryLevel << ")");
        return DcmQRSqlDatabaseError ;
    }

    if (queryLevel > lowestLevel) {
        DCMQRDB_DEBUG("Level incompatible with Information Model (level " << (int)queryLevel << ")");
        return DcmQRSqlDatabaseError ;
    }

    for (level = PATIENT_LEVEL ; level <= IMAGE_LEVEL ; level++) {

        /**** Manage exception due to StudyRoot Information Model :
        **** In this information model, move may not include any
        **** Patient attributes.
        ***/

        if ((level == PATIENT_LEVEL) && (infLevel == STUDY_LEVEL)) {

            /** In Study Root Information Model, do not accept any
            ** Patient Tag
            */

            int atLeastOneKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                atLeastOneKeyFound = OFTrue ;
            }
            if (atLeastOneKeyFound) {
                DCMQRDB_DEBUG("Key found in Study Root Information Model (level " << level << ")");
                return DcmQRSqlDatabaseError ;
            }
        }

        /**** If current level is above or equal to the QueryLevel
        ***/

        else if (level <= queryLevel) {

            /** For these levels, only unique keys are allowed
            ** Parse the request list elements reffering to
            ** this level.
            ** Check that only unique key attr are provided
            */

            int uniqueKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                DB_GetTagKeyAttr (plist->elem. XTag, &XTagType) ;
                if (XTagType != UNIQUE_KEY) {
                    DCMQRDB_DEBUG("Non Unique Key found (level " << level << ")");
                    return DcmQRSqlDatabaseError ;
                }
                else if (uniqueKeyFound) {
                    DCMQRDB_DEBUG("More than one Unique Key found (level " << level << ")");
                    return DcmQRSqlDatabaseError ;
                }
                else
                    uniqueKeyFound = OFTrue ;
            }
            if (! uniqueKeyFound) {
                DCMQRDB_DEBUG("No Unique Key found (level " << level << ")");
                return DcmQRSqlDatabaseError ;
            }
        }

        /**** If current level beyond the QueryLevel
        ***/

        else if (level > queryLevel) {

            /** For this level, no key is allowed
            ** Parse the request list elements reffering to
            ** this level.
            ** Check that no key is provided
            */

            int atLeastOneKeyFound = OFFalse ;
            for (plist = findRequestList ; plist ; plist = plist->next) {
                DB_GetTagLevel (plist->elem. XTag, &XTagLevel) ;
                if (XTagLevel != level)
                    continue ;
                atLeastOneKeyFound = OFTrue ;
            }
            if (atLeastOneKeyFound) {
                DCMQRDB_DEBUG("Key found beyond query level (level " << level << ")");
                return DcmQRSqlDatabaseError ;
            }
        }

    }
    return EC_Normal ;
}



OFCondition DcmQueryRetrieveSqlDatabaseHandle::startMoveRequest(
        const char      *SOPClassUID,
        DcmDataset      *moveRequestIdentifiers,
        DcmQueryRetrieveDatabaseStatus  *status)
{

    DB_SmallDcmElmt     elem ;
    DB_ElementList      *plist = NULL;
    DB_ElementList      *last = NULL;
    DB_CounterList      *pidxlist = NULL;
    DB_CounterList      *lastidxlist = NULL;
    int                 MatchFound = OFFalse;
    IdxRecord           idxRec ;
    DB_LEVEL            qLevel = PATIENT_LEVEL; // highest legal level for a query in the current model
    DB_LEVEL            lLevel = IMAGE_LEVEL;   // lowest legal level for a query in the current model
    OFCondition         cond = EC_Normal;
    OFBool qrLevelFound = OFFalse;
    std::string         lSql = "";
    std::string         lTag = "";

    /**** Is SOPClassUID supported ?
    ***/

    if (strcmp( SOPClassUID, UID_MOVEPatientRootQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = PATIENT_ROOT ;
    else if (strcmp( SOPClassUID, UID_MOVEStudyRootQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = STUDY_ROOT ;
#ifndef NO_PATIENTSTUDYONLY_SUPPORT
    else if (strcmp( SOPClassUID, UID_RETIRED_MOVEPatientStudyOnlyQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = PATIENT_STUDY ;
#endif
#ifndef NO_GET_SUPPORT
    /* experimental support for GET */
    else if (strcmp( SOPClassUID, UID_GETPatientRootQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = PATIENT_ROOT ;
    else if (strcmp( SOPClassUID, UID_GETStudyRootQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = STUDY_ROOT ;
#ifndef NO_PATIENTSTUDYONLY_SUPPORT
    else if (strcmp( SOPClassUID, UID_RETIRED_GETPatientStudyOnlyQueryRetrieveInformationModel) == 0)
        handle_->rootLevel = PATIENT_STUDY ;
#endif
#endif

    else {
        status->setStatus(STATUS_MOVE_Failed_SOPClassNotSupported);
        return (DcmQRSqlDatabaseError) ;
    }

    /**** Parse Identifiers in the Dicom Object
    **** Find Query Level and contruct a list
    **** of query identifiers
    ***/

    int elemCount = (int)(moveRequestIdentifiers->card());
    for (int elemIndex=0; elemIndex<elemCount; elemIndex++) {

        DcmElement* dcelem = moveRequestIdentifiers->getElement(elemIndex);

        elem.XTag = dcelem->getTag().getXTag();
        if (elem.XTag == DCM_QueryRetrieveLevel || DB_TagSupported(elem.XTag)) {
            elem.ValueLength = dcelem->getLength();
            if (elem.ValueLength == 0) {
                elem.PValueField = NULL ;
            } else if ((elem.PValueField = (char*)malloc((size_t)(elem.ValueLength+1))) == NULL) {
                status->setStatus(STATUS_MOVE_Failed_UnableToProcess);
                return (DcmQRSqlDatabaseError) ;
            } else {
                /* only char string type tags are supported at the moment */
                char *s = NULL;
                dcelem->getString(s);
                strcpy(elem.PValueField, s);
            }

            /** If element is the Query Level, store it in handle
             */

            if (elem. XTag == DCM_QueryRetrieveLevel) {
                char *pc ;
                char level [50] ;

                strncpy (level, (char *) elem. PValueField, (size_t)((elem. ValueLength < 50) ? elem. ValueLength : 49)) ;

                /*** Skip this two lines if you want strict comparison
                **/

                for (pc = level ; *pc ; pc++)
                    *pc = ((*pc >= 'a') && (*pc <= 'z')) ? 'A' - 'a' + *pc : *pc ;

                if (strncmp (level, PATIENT_LEVEL_STRING,
                             strlen (PATIENT_LEVEL_STRING)) == 0)
                    handle_->queryLevel = PATIENT_LEVEL ;
                else if (strncmp (level, STUDY_LEVEL_STRING,
                                  strlen (STUDY_LEVEL_STRING)) == 0)
                    handle_->queryLevel = STUDY_LEVEL ;
                else if (strncmp (level, SERIE_LEVEL_STRING,
                                  strlen (SERIE_LEVEL_STRING)) == 0)
                    handle_->queryLevel = SERIE_LEVEL ;
                else if (strncmp (level, IMAGE_LEVEL_STRING,
                                  strlen (IMAGE_LEVEL_STRING)) == 0)
                    handle_->queryLevel = IMAGE_LEVEL ;
                else {
#ifdef DEBUG
                    DCMQRDB_DEBUG("DB_startMoveRequest : STATUS_MOVE_Failed_UnableToProcess");
#endif
                    status->setStatus(STATUS_MOVE_Failed_UnableToProcess);
                    return (DcmQRSqlDatabaseError) ;
                }
                qrLevelFound = OFTrue;
            } else {
                /** Else it is a query identifier
                ** Append it to our RequestList
                */
                if (! DB_TagSupported (elem. XTag))
                    continue ;

                plist = (DB_ElementList *) malloc (sizeof( DB_ElementList ) ) ;
                if (plist == NULL) {
                    status->setStatus(STATUS_FIND_Refused_OutOfResources);
                    return (DcmQRSqlDatabaseError) ;
                }
                plist->next = NULL ;
                DB_DuplicateElement (&elem, & (plist->elem)) ;
                if (handle_->findRequestList == NULL) {
                    handle_->findRequestList = last = plist ;
                } else {
                    last->next = plist ;
                    last = plist ;
                }
            }

            if ( elem. PValueField ) {
                free (elem. PValueField) ;
            }
        }
    }

    if (!qrLevelFound) {
        /* The Query/Retrieve Level is missing */
        status->setStatus(STATUS_MOVE_Failed_IdentifierDoesNotMatchSOPClass);
        DCMQRDB_WARN("DB_startMoveRequest(): missing Query/Retrieve Level");
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        return (DcmQRSqlDatabaseError) ;
    }

    switch (handle_->rootLevel)
    {
      case PATIENT_ROOT :
        qLevel = PATIENT_LEVEL ;
        lLevel = IMAGE_LEVEL ;
        break ;
      case STUDY_ROOT :
        qLevel = STUDY_LEVEL ;
        lLevel = IMAGE_LEVEL ;
        break ;
      case PATIENT_STUDY:
        qLevel = PATIENT_LEVEL ;
        lLevel = STUDY_LEVEL ;
        break ;
    }

    /**** Test the consistency of the request list
    ***/
    if (doCheckMoveIdentifier) {
        cond = testMoveRequestList (handle_->findRequestList,
                                    handle_->queryLevel, qLevel, lLevel) ;
        if (cond != EC_Normal) {
            handle_->idxCounter = -1 ;
            DB_FreeElementList (handle_->findRequestList) ;
            handle_->findRequestList = NULL ;
            status->setStatus(STATUS_MOVE_Failed_IdentifierDoesNotMatchSOPClass);
            return (cond) ;
        }
    }

    /**** Goto the beginning of Index File
    **** Then find all matching images
    ***/

    MatchFound = OFFalse ;
    handle_->moveCounterList = NULL ;
    handle_->NumberRemainOperations = 0 ;

    /**** Se obtiene el SQL para realizar la búsqueda para hacer el Retrieve ***/
    lSql = getCMoveSql(handle_->findRequestList, handle_->queryLevel);

    std::string lConn;
    lConn = connectionString_;

    PGconn *dbconn = PQconnectdb(lConn.c_str());
    if(PQstatus(dbconn) == CONNECTION_OK) 
    {
      PQexec(dbconn, CLIENT_ENCODING);

      internalResult = PQexec(dbconn, lSql.c_str());
      queryPos = 0;
      if (PQresultStatus(internalResult) != PGRES_TUPLES_OK)
      {
        PQclear(internalResult);
        DCMQRDB_ERROR("Error." << PQerrorMessage(dbconn));
        PQfinish(dbconn);
        return(DcmQRSqlDatabaseError); 
      };

      if(PQntuples(internalResult) == 0)
      {
        DCMQRDB_DEBUG("No matches found.");
        MatchFound = OFFalse;
        cond = EC_Normal;
      }
      else
      {
        MatchFound = OFTrue;
        cond = EC_Normal;

        // Creamos moveCounterList
        for (int rsit = 0; rsit < PQntuples(internalResult); rsit++)
        {
          pidxlist = (DB_CounterList *) malloc (sizeof( DB_CounterList ) ) ;
          if (pidxlist == NULL) {
              status->setStatus(STATUS_FIND_Refused_OutOfResources);
              return (DcmQRSqlDatabaseError) ;
          }

          pidxlist->next = NULL ;
          pidxlist->idxCounter = handle_->idxCounter ;
          handle_->NumberRemainOperations++ ;
          if ( handle_->moveCounterList == NULL )
              handle_->moveCounterList = lastidxlist = pidxlist ;
          else {
              lastidxlist->next = pidxlist ;
              lastidxlist = pidxlist ;
          }
        }
      }
    }
    else
    {
      DCMQRDB_ERROR(PQerrorMessage(dbconn));
      MatchFound = OFFalse ;
      return (DcmQRSqlDatabaseError) ;
    }
    PQfinish(dbconn);


    /**** If an error occured in Matching function
    ****    return a failed status
    ***/

    if (cond != EC_Normal) {
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        status->setStatus(STATUS_MOVE_Failed_UnableToProcess);
        return (cond) ;
    }


    /**** If a matching image has been found,
    ****         add index record to UID found list
    ****    prepare Response List in handle
    ****    return status is pending
    ***/
    if (MatchFound) {
        status->setStatus(STATUS_Pending);
        return (EC_Normal) ;
    }
    /**** else no matching image has been found,
    ****    free query identifiers list
    ****    status is success
    ***/

    else {
        handle_->idxCounter = -1 ;
        DB_FreeElementList (handle_->findRequestList) ;
        handle_->findRequestList = NULL ;
        status->setStatus(STATUS_Success);
        return (EC_Normal) ;
    }
}

OFCondition DcmQueryRetrieveSqlDatabaseHandle::nextMoveResponse(
                char            *SOPClassUID,
                char            *SOPInstanceUID,
                char            *imageFileName,
                unsigned short  *numberOfRemainingSubOperations,
                DcmQueryRetrieveDatabaseStatus  *status)
{
    DB_CounterList              *nextlist ;

    /**** If all matching images have been retrieved,
    ****    status is success
    ***/
    if ( handle_->NumberRemainOperations <= 0 ) {
        status->setStatus(STATUS_Success);
        return (EC_Normal) ;
    }

    strcpy (SOPClassUID, PQgetvalue(internalResult, queryPos, PQfnumber(internalResult, "SOPClassUID")));
    strcpy (SOPInstanceUID, PQgetvalue(internalResult, queryPos, PQfnumber(internalResult, "SOPInstanceUID")));
    strcpy (imageFileName, PQgetvalue(internalResult, queryPos, PQfnumber(internalResult, "ImagePath")));
    *numberOfRemainingSubOperations = --handle_->NumberRemainOperations ;
    queryPos = ++queryPos;

    nextlist = handle_->moveCounterList->next ;
    free (handle_->moveCounterList) ;
    handle_->moveCounterList = nextlist ;

    status->setStatus(STATUS_Pending);
    return (EC_Normal) ;
}

OFCondition DcmQueryRetrieveSqlDatabaseHandle::cancelMoveRequest (DcmQueryRetrieveDatabaseStatus *status)
{
    DB_CounterList *plist ;

    while (handle_->moveCounterList) {
        plist  = handle_->moveCounterList ;
        handle_->moveCounterList = handle_->moveCounterList->next ;
        free (plist) ;
    }

    status->setStatus(STATUS_MOVE_Cancel_SubOperationsTerminatedDueToCancelIndication);

    DB_unlock();

    return (EC_Normal) ;
}


/* ========================= STORE ========================= */


void DcmQueryRetrieveSqlDatabaseHandle::enableQuotaSystem(OFBool enable)
{
    quotaSystemEnabled = enable;
}


/*
** Image file deleting
*/


OFCondition DcmQueryRetrieveSqlDatabaseHandle::deleteImageFile(char* imgFile)
{
    if (!quotaSystemEnabled) {
      DCMQRDB_WARN("file delete operations are disabled, keeping file: " << imgFile << " despite duplicate SOP Instance UID.");
      return EC_Normal;
    } else {
      DCMQRDB_WARN("Deleting file: " << imgFile << " due to quota or duplicate SOP instance UID.");
    }

#ifdef LOCK_IMAGE_FILES
    int lockfd;
#ifdef O_BINARY
    lockfd = open(imgFile, O_RDWR | O_BINARY, 0666);    /* obtain file descriptor */
#else
    lockfd = open(imgFile, O_RDWR, 0666);   /* obtain file descriptor */
#endif
    if (lockfd < 0) {
      DCMQRDB_WARN("DB ERROR: cannot open image file for deleting: " << imgFile);
      return DcmQRSqlDatabaseError;
    }
    if (dcmtk_flock(lockfd, LOCK_EX) < 0) { /* exclusive lock (blocking) */
      DCMQRDB_WARN("DB ERROR: cannot lock image file  for deleting: " << imgFile);
      dcmtk_plockerr("DB ERROR");
    }
#endif

    if (unlink(imgFile) < 0) {
        /* delete file */
        DCMQRDB_ERROR("DB ERROR: cannot delete image file: " << imgFile << OFendl
            << "DcmQRSqlDatabaseError: " << strerror(errno));
    }

#ifdef LOCK_IMAGE_FILES
    if (dcmtk_flock(lockfd, LOCK_UN) < 0) { /* unlock */
        DCMQRDB_WARN("DB ERROR: cannot unlock image file  for deleting: " << imgFile);
        dcmtk_plockerr("DB ERROR");
     }
    close(lockfd);              /* release file descriptor */
#endif

    return EC_Normal;
}

/*************************
 *   Verify if study UID already exists
 *   If the study UID exists, its index in the study descriptor is returned.
 *   If the study UID does not exist, the index of the first unused descriptor entry is returned.
 *   If no entries are free, maxStudiesAllowed is returned.
 */

int DcmQueryRetrieveSqlDatabaseHandle::matchStudyUIDInStudyDesc (StudyDescRecord *pStudyDesc, char *StudyUID, int maxStudiesAllowed)
{
    int s = 0 ;
    while  (s < maxStudiesAllowed)
    {
      if ((pStudyDesc[s].NumberofRegistratedImages > 0) && (0 == strcmp(pStudyDesc[s].StudyInstanceUID, StudyUID))) break;
      s++ ;
    }
    if (s==maxStudiesAllowed) // study uid does not exist, look for free descriptor
    {
      s=0;
      while  (s < maxStudiesAllowed)
      {
        if (pStudyDesc[s].NumberofRegistratedImages == 0) break;
        s++ ;
      }
    }
    return s;
}

/*************************
**  Add data from imageFileName to database
 */

void escapestring(OFString &s)
{
  OFString tmp = "";
  for(unsigned int i=0; i<s.length(); i++)
  {
    tmp += s[i];
    if(s[i] == '\'')
    {
      tmp += s[i];
    }
  }
  s = tmp;
}

OFCondition DcmQueryRetrieveSqlDatabaseHandle::storeRequest (
    const char  *SOPClassUID,
    const char  * /*SOPInstanceUID*/,
    const char  *imageFileName,
    DcmQueryRetrieveDatabaseStatus   *status,
    OFBool      isNew)
{
    IdxRecord           idxRec;
    int                 i;
    OFString            tagValue;

    /**** Initialize an IdxRecord
    ***/
    OFString  DBResult="";
    bzero((char*)&idxRec, sizeof(idxRec));

    DB_IdxInitRecord (&idxRec, 0) ;

    strncpy(idxRec.filename, imageFileName, DBC_MAXSTRING);

    strncpy (idxRec.SOPClassUID, SOPClassUID, UI_MAX_LENGTH);

    /**** Get IdxRec values from ImageFile
    ***/

    DcmFileFormat dcmff;
    if (dcmff.loadFile(imageFileName).bad())
    {
      DCMQRDB_WARN("DB: Cannot open file: " << imageFileName << ": "
          << strerror(errno));
      status->setStatus(STATUS_STORE_Error_CannotUnderstand);
      return (DcmQRSqlDatabaseError) ;
    }

    DcmDataset *dset = dcmff.getDataset();

    /* InstanceDescription */
    OFBool useDescrTag = OFTrue;
    DcmTagKey descrTag = DCM_ImageComments;

    if (SOPClassUID != NULL)
    {
      /* Chequeamos si el SOPClassUID está permitido
         Si no lo está retornamos EC_Normal para que
         el sender continúe enviando.

         Todo: luego tenemos que guardar los datos en una estructura
         del tipo Patient->Study->Other donde se guarden los
         objetos que no son del tipo imagen.
      */
      if ((strcmp(SOPClassUID, UID_BasicTextSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_EnhancedSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_ComprehensiveSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_ProcedureLogStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_MammographyCADSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_KeyObjectSelectionDocumentStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_ChestCADSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_ColonCADSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_XRayRadiationDoseSRStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_SpectaclePrescriptionReportStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_MacularGridThicknessAndVolumeReportStorage) == 0) ||
                   (strcmp(SOPClassUID, UID_ImplantationPlanSRDocumentStorage) == 0))
      {
        DCMQRDB_INFO("Invalid SOP Class: " << SOPClassUID);
        //status->setStatus(STATUS_Success);
        return (EC_Normal);
      }
    }
  
    //Determina el tamaño del archivo DICOM, para ser almacenado en la BD.
    long begin,end, fileSize;
    std::ifstream myfile(imageFileName);
    begin = myfile.tellg();
    myfile.seekg(0,std::ios::end);
    end = myfile.tellg();
    myfile.close();
    fileSize= (end-begin);//en bytes
    std::stringstream ssFileSize;
    ssFileSize << fileSize;

    // Guarda en base de datos
    std::string lConn;
    lConn = connectionString_;
    PGconn *dbconn = PQconnectdb(lConn.c_str());
    if(PQstatus(dbconn) == CONNECTION_OK) 
    {
      DCMQRDB_INFO("Connected to database!");
      PQexec(dbconn, CLIENT_ENCODING);
      // iniciamos una transacción
      PQexec(dbconn, "BEGIN");

      std::stringstream sql;
      sql << "select storeimage_experimental(";

      dset->findAndGetOFString(DCM_PatientBirthDate, tagValue);
      if(tagValue != "")
      {
        sql << "'" << tagValue << "',";
      }
      else
        sql << "null,";

      dset->findAndGetOFString(DCM_PatientBirthTime, tagValue);
      if(tagValue != "")
      {
        sql << "'" << tagValue << "',";
      }
      else
        sql << "null,";

      dset->findAndGetOFString(DCM_PatientSex, tagValue);
      sql <<  "'" << tagValue << "',";


      dset->findAndGetOFString(DCM_PatientName, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_PatientID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_OtherPatientIDs, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_OtherPatientNames, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_EthnicGroup, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_PatientAge, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_PatientSize, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_PatientWeight, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_Occupation, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_StudyDate, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_StudyTime, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_StudyID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_StudyDescription, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_InstitutionName, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_Modality, tagValue);
      sql <<  "'" << tagValue << "',";

      // Calling AETitle
      sql << "'" << getCallingAETitle() << "'," ;

      dset->findAndGetOFString(DCM_NameOfPhysiciansReadingStudy, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_AccessionNumber, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_ReferringPhysiciansName, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_StudyInstanceUID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_SeriesNumber, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_SeriesInstanceUID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_SOPInstanceUID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_SeriesDate, tagValue);
      if(tagValue.length() > 0)
        sql <<  "'" << tagValue << "',";
      else
        sql <<  "null,";
      dset->findAndGetOFString(DCM_SeriesTime, tagValue);
      if(tagValue.length() > 0)
        sql <<  "'" << tagValue << "',";
      else
        sql <<  "null,";
      dset->findAndGetOFString(DCM_SeriesDescription, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      //BodyPartExamined
      dset->findAndGetOFString(DCM_BodyPartExamined, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_ProtocolName, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_OperatorsName, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_PerformingPhysiciansName, tagValue);
      escapestring(tagValue);
      sql <<  "'" << tagValue << "',";

      //Rows
      dset->findAndGetOFString(DCM_Rows, tagValue);
        if(tagValue!= "")
                {
                sql << tagValue << ",";
                }
      else
                {
                sql << "" << ",";
                }


      //Columns
      dset->findAndGetOFString(DCM_Columns, tagValue);
      if(tagValue!= "")
                {
                sql <<  tagValue << ",";
                }
      else
                {
                sql << "" << ",";
                }


      //Se agrega el tamaño del archivo
      sql << "'" << ssFileSize.str() << "',";

      dset->findAndGetOFString(DCM_ContentLabel, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_InstanceNumber, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_ImageType, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_SOPClassUID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_AcquisitionDate, tagValue);
      if(tagValue.length() > 0)
        sql <<  "'" << tagValue << "',";
      else
        sql <<  "null,";

      dset->findAndGetOFString(DCM_AcquisitionTime, tagValue);
      if(tagValue.length() > 0)
        sql <<  "'" << tagValue << "',";
      else
        sql <<  "null,";

      dset->findAndGetOFString(DCM_ContentDate, tagValue);
      if(tagValue.length() > 0)
        sql <<  "'" << tagValue << "',";
      else
        sql <<  "null,";
      dset->findAndGetOFString(DCM_ContentTime, tagValue);
      if(tagValue.length() > 0)
        sql <<  "'" << tagValue << "',";
      else
        sql <<  "null,";

      dset->findAndGetOFString(DCM_AcquisitionNumber, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_ImagePositionPatient, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_ImageOrientationPatient, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_FrameOfReferenceUID, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_SliceLocation, tagValue);
      sql <<  "'" << tagValue << "',";

      dset->findAndGetOFString(DCM_NumberOfFrames, tagValue);
      sql << "'" << tagValue << "',";
      sql << "'" << imageFileName << "',";
      dset->findAndGetOFString(DCM_PatientPosition, tagValue);
      sql << "'" << tagValue << "'";
      sql << ");";

      // se reemplazan los '' por null
      int uPos = 0;
      for( ;(uPos = sql.str().find( "''", uPos )) != std::string::npos; )
      {
          sql.str().replace( uPos, 2, "null" );
          uPos += 4;
      }
      DCMQRDB_INFO(sql.str());
      std::string lString = sql.str();
      if( (internalResult = PQexec(dbconn, lString.c_str())) == NULL)
      {
          DCMQRDB_ERROR(PQerrorMessage(dbconn));
          status->setStatus(STATUS_STORE_Error_CannotUnderstand);
          PQfinish(dbconn); 
          return (DcmQRSqlDatabaseError) ;
      }
      
    DBResult = PQgetvalue(internalResult, 0, 0);
    if( DBResult == "DONE")
     {
     DCMQRDB_INFO("DBstatus operations [OK]");
     }
     else
     {
     DCMQRDB_ERROR("DBstatus operations "<< DBResult);
     DCMQRDB_ERROR(PQerrorMessage(dbconn));
     status->setStatus(STATUS_STORE_Error_CannotUnderstand);
     PQfinish(dbconn);
     return (DcmQRSqlDatabaseError);
     }
      DCMQRDB_INFO("Commit taking place ..");
      PQexec(dbconn, "COMMIT");
      PQfinish(dbconn); 
      return (EC_Normal) ;
    }
    else
    {
      DCMQRDB_ERROR(PQerrorMessage(dbconn));
      status->setStatus(STATUS_STORE_Error_CannotUnderstand);
      PQfinish(dbconn); 
      return (DcmQRSqlDatabaseError) ;
    };
}

/*
** Prune invalid DB records.
*/

OFCondition DcmQueryRetrieveSqlDatabaseHandle::pruneInvalidRecords()
{
    // esta función es un residuo de la clase que trabaja
    // con Index file en vez de base de datos.
    // No hacemos nada aqui.
    return EC_Normal;
}


/* ========================= UTILS ========================= */


const char *DcmQueryRetrieveSqlDatabaseHandle::getStorageArea() const
{
    return handle_->storageArea;
}

void DcmQueryRetrieveSqlDatabaseHandle::setIdentifierChecking(OFBool checkFind, OFBool checkMove)
{
    doCheckFindIdentifier = checkFind;
    doCheckMoveIdentifier = checkMove;
}


/***********************
 *      Creates a handle
 */

DcmQueryRetrieveSqlDatabaseHandle::DcmQueryRetrieveSqlDatabaseHandle(
    const char *AcallingAETitle,
    const char *storageArea,
    long maxStudiesPerStorageArea,
    long maxBytesPerStudy,
    const char *connectionString,
    OFCondition& result)
: handle_(NULL)
, quotaSystemEnabled(OFTrue)
, doCheckFindIdentifier(OFFalse)
, doCheckMoveIdentifier(OFFalse)
, fnamecreator()
{
    callingAETitle = AcallingAETitle;

    handle_ = new DB_Private_Handle;

    result = EC_Normal;
    // se asigna el storage area
    sprintf (handle_ -> storageArea,"%s", storageArea);
    // se asigna el connection string
    sprintf (connectionString_, "%s", connectionString);
    return;
}

const char* DcmQueryRetrieveSqlDatabaseHandle::getCallingAETitle()
{
  return callingAETitle;
}

/***********************
 *      Destroys a handle
 */

DcmQueryRetrieveSqlDatabaseHandle::~DcmQueryRetrieveSqlDatabaseHandle()
{
    if (handle_)
    {
      //closeresult = close( handle_ -> pidx);
      /* Free lists */
      DB_FreeElementList (handle_ -> findRequestList);
      DB_FreeElementList (handle_ -> findResponseList);
      DB_FreeUidList (handle_ -> uidList);
      delete handle_;
    }
}

/**********************************
 *      Provides a storage filename
 */

OFString getDate()                                                                                                                                           
{
    char buf[128];
    struct tm *today;
    time_t now;

    time(&now);
    today=localtime(&now);

    strftime(buf,128,"%d-%m-%Y",today);
    return buf;
}

OFString getDcmqrscpVersion()
{
 OFString  version;
 version = "2.0.1 Beta";
 return version;
}



OFCondition DcmQueryRetrieveSqlDatabaseHandle::makeNewStoreFileName(
                const char      *SOPClassUID,
                const char      *SOPInstanceUID,
                const char      *StudyInstanceUID,
                char            *newImageFileName)
{
    DCMQRDB_INFO("======================= G-PacsInfoSystem =======================");
    DCMQRDB_INFO("Version dcmqrscp: " << getDcmqrscpVersion());
    DCMQRDB_INFO("=======================      G-Pacs      =======================");

    OFString filename;
    char prefix[12];
    const char *m = dcmSOPClassUIDToModality(SOPClassUID);
    if (m==NULL) m = "XX";
    sprintf(prefix, "%s_", m);
    unsigned int seed = (unsigned int)time(NULL);
    newImageFileName[0]=0; // return empty string in case of error
    OFString subdirectoryName = getStorageArea();
    DCMQRDB_INFO("General dicom storage directory: " << subdirectoryName);
    subdirectoryName += PATH_SEPARATOR;
    subdirectoryName +=getCallingAETitle();
    DCMQRDB_INFO("Using callingAETitle as name for a new subdirectory: " << subdirectoryName);
    DCMQRDB_DEBUG("Checking for directory: " << subdirectoryName); 
    if( !OFStandard::dirExists(subdirectoryName) ) 
    { 
      DCMQRDB_DEBUG("Directory: " << subdirectoryName << " does not exists. Creating..."); 
      // if it does not exist create it 
      #ifdef HAVE_WINDOWS_H 
      if( _mkdir( subdirectoryName.c_str() ) == -1 ) 
      #else 
      if( mkdir( subdirectoryName.c_str(), S_IRWXU | S_IRWXG | S_IRWXO ) == -1 ) 
      #endif 
      { 
        DCMQRDB_ERROR("creating new subdirectory for study: " << subdirectoryName); 
        return DcmQRSqlDatabaseError;
      } 
    } 
    subdirectoryName += PATH_SEPARATOR;
    subdirectoryName += getDate();
    DCMQRDB_INFO("Using date as name for a new subdirectory: " << subdirectoryName);
    DCMQRDB_DEBUG("Checking for directory: " << subdirectoryName); 
    if( !OFStandard::dirExists(subdirectoryName) ) 
    { 
     DCMQRDB_DEBUG("Directory: " << subdirectoryName << " does not exists. Creating..."); 
     // if it does not exist create it 
     #ifdef HAVE_WINDOWS_H 
     if( _mkdir( subdirectoryName.c_str() ) == -1 ) 
     #else 
       if( mkdir( subdirectoryName.c_str(), S_IRWXU | S_IRWXG | S_IRWXO ) == -1 ) 
          #endif 
         { 
           DCMQRDB_ERROR("creating new subdirectory for study: " << subdirectoryName); 
           return DcmQRSqlDatabaseError;
         } 
    }
    DCMQRDB_INFO("Final subdirectory: " << subdirectoryName); 
  
   filename += subdirectoryName;
   filename += PATH_SEPARATOR;
   filename += prefix;
   filename += SOPInstanceUID;
   filename +=".dcm";
   
  if(OFStandard::fileExists(filename))  
   {
     DCMQRDB_INFO("[FE00001] File already exists. It will be replaced. "); 
     
    
     //DCMQRDB_INFO("creating new filename...");
     /*
     do
      {
      if (! fnamecreator.makeFilename(seed, subdirectoryName.c_str(), prefix, ".dcm", filename))
      return DcmQRSqlDatabaseError;
       if(OFStandard::fileExists(filename))
        {
         DCMQRDB_INFO("[FE00001] File already exists ["<<filename<<"]"); 
         DCMQRDB_INFO("creating new filename...");
        }
      }
     while(OFStandard::fileExists(filename));
     */
   }
                                                                 
 

   DCMQRDB_INFO("Saving file: " << filename.c_str());
    strcpy(newImageFileName, filename.c_str());
    return EC_Normal;
}


/***********************
 *    Default constructors for struct IdxRecord and DB_SSmallDcmElmt
 */


IdxRecord::IdxRecord()
: RecordedDate(0.0)
, ImageSize(0)
, hstat(DVIF_objectIsNotNew)
{
}

DB_SmallDcmElmt::DB_SmallDcmElmt()
: PValueField(NULL)
, ValueLength(0)
, XTag()
{
}


DcmQueryRetrieveSqlDatabaseHandleFactory::DcmQueryRetrieveSqlDatabaseHandleFactory(const DcmQueryRetrieveConfig *config)
: DcmQueryRetrieveDatabaseHandleFactory()
, config_(config)
{
}

DcmQueryRetrieveSqlDatabaseHandleFactory::~DcmQueryRetrieveSqlDatabaseHandleFactory()
{
}


DcmQueryRetrieveDatabaseHandle *DcmQueryRetrieveSqlDatabaseHandleFactory::createDBHandle(
    const char *callingAETitle,
    const char *calledAETitle,
    OFCondition& result) const
{

  return new DcmQueryRetrieveSqlDatabaseHandle(
    callingAETitle,
    config_->getStorageArea(calledAETitle),
    config_->getMaxStudies(calledAETitle),
    config_->getMaxBytesPerStudy(calledAETitle),
    config_->getConnectionString(),
    result);
}

bool estado(std::string AKey) {
    using std::cout;
    using std::cerr;

    //cout << "C++ dlopen demo\n\n";

    // open the library
    //cout << "Opening validateFingerPrintAlg2.so...\n";
    void* handle = dlopen("./libfingerprint.so", RTLD_LAZY);

    if (!handle) {
        cerr << "Cannot open library: " << dlerror() << '\n';
        return 1;
    }

    // load the symbol
    //cout << "Loading symbol validateFingerPrintAlg2...\n";


    validateFingerPrintAlg2_t validateFingerPrintAlg2 = (validateFingerPrintAlg2_t) dlsym(handle, "validateFingerPrintAlg2");
    if (!validateFingerPrintAlg2) {
        cerr << "Cannot load symbol 'validateFingerPrintAlg2': " << dlerror() <<
            '\n';
        dlclose(handle);
        return 1;
    }

    // use it to do the calculation
    //cout << "Calling validateFingerPrintAlg2...\n";
    if(validateFingerPrintAlg2(AKey.c_str()) == true)
      return true;
    else
      return false;

    // close the library
    //cout << "Closing library...\n";
    dlclose(handle);
}


std::string getFingerPrint() {
    using std::cout;
    using std::cerr;
    const char * Avar;
    //cout << "C++ dlopen demo\n\n";

    // open the library
    //cout << "Opening validateFingerPrintAlg2.so...\n";
    void* handle = dlopen("./libfingerprint.so", RTLD_LAZY);

    if (!handle) {
        cerr << "Cannot open library: " << dlerror() << '\n';
        return "error";
    }

    // load the symbol
    //cout << "Loading symbol createFingerPrintAlg1...\n";


    createFingerPrintAlg1_t createFingerPrintAlg1 = ((createFingerPrintAlg1_t) dlsym(handle, "createFingerPrintAlg1"));
    if (!createFingerPrintAlg1) {
        cerr << "Cannot load symbol 'createFingerPrintAlg1': " << dlerror() <<
            '\n';
        dlclose(handle);
        return "error cannot load simbol";
    }

    // use it to do the calculation
    //cout << "Calling createFingerPrintAlg1...\n";
    return createFingerPrintAlg1(Avar);
    // close the library
    //cout << "Closing library...\n";
    dlclose(handle);
}
