# ====================
# Anonymize DICOM data
# ====================

import csv
import sys
import pydicom
import pydicom.charset
import pydicom.config
from pydicom import compat
from pydicom._version import __version_info__
from pydicom.charset import default_encoding, convert_encodings
from pydicom.config import logger
from pydicom.datadict import dictionary_VR
from pydicom.datadict import (tag_for_keyword, keyword_for_tag, repeater_has_keyword)
from pydicom.dataelem import DataElement, DataElement_from_raw, RawDataElement
from pydicom.pixel_data_handlers.util import (convert_color_space, reshape_pixel_array)
import pydicom.pixel_data_handlers.gdcm_handler as gdcm_handler
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydicom.tag import Tag, BaseTag, tag_in_exception
from pydicom.uid import (ExplicitVRLittleEndian, ImplicitVRLittleEndian, ExplicitVRBigEndian,
                         PYDICOM_IMPLEMENTATION_UID)
import os
import errno
import datetime
from datetime import date
from joblib import Parallel, delayed


#######################
#######################
# USEFUL FUNCTIONS
#######################
#######################

# To replace multiple elements
def replace_multiple(main_string, to_be_replaces, new_string):
    # Iterate over the strings to be replaced
    for elem in to_be_replaces:
        # Check if string is in the main string
        if elem in main_string:
            # Replace the string
            main_string = main_string.replace(elem, new_string)
    return main_string


# To remove weird chars
def to_pretty_string(string_to_prettify):
    string_to_prettify = replace_multiple(string_to_prettify, [":", "*", "/", "\\", "\"", "<", ">", "|", "?"], "")
    string_to_prettify = replace_multiple(string_to_prettify, ["^"], " ")
    return string_to_prettify


# Anonymise a single DICOM
# def anonymiseDICOM(patient_ipp, originalFile, destinationFolder, daysElapsed, modalities, series): # OLD
def anonymise_dicom(image_to_anonymize_data):

    # Get image_to_anonymize_data
    ano_patient_ipp = image_to_anonymize_data[0]
    original_file = image_to_anonymize_data[1]
    destination_folder = image_to_anonymize_data[2]
    days_elapsed = image_to_anonymize_data[3]
    try:
        ano_series = image_to_anonymize_data[4]
    except:
        ano_series = ""
        pass

    # init log_lines = [] to return
    log_lines = []

    SEPARATOR = ";"
    ENDER = "\n"

    try:

        # Load DICOM image as dataset
        print('Reading file', originalFile)
        today = date.today()
        d1 = today.strftime("%Y%m%d")
        dataset = pydicom.dcmread(original_file)

        # Find the name of the study
        studyDescription = "unnamedStudy"
        if hasattr(dataset, 'StudyDescription') or hasattr(dataset, 'Study Description'):
            studyDescriptionChallenger = dataset.StudyDescription
            if len(studyDescriptionChallenger) > 0:
                studyDescription = studyDescriptionChallenger
        if (studyDescription == "unnamedStudy" and (
                hasattr(dataset, 'StudyID') or hasattr(dataset, 'Study ID'))):
            studyDescriptionChallenger = str(dataset.StudyID)
            if len(studyDescriptionChallenger) > 0:
                studyDescription = studyDescriptionChallenger

        LOG_TYPE = "STUDY_DESCRIPTION"
        LOGG = studyDescription
        log_lines.append(LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)

        # Continue only if there is a study description, otherwise it might be a screen save
        if studyDescription != "unnamedStudy":

            # Find the number of series, it has to be the same as the variable 'series' given to this function
            seriesOk = False

            series_list = []
            if len(ano_series) == 0:
                seriesOk = True  # no filter
            else:
                if hasattr(dataset, 'SeriesNumber') or hasattr(dataset, 'Series Number'):
                    dicom_series = str(dataset.SeriesNumber)
                    series_list = ano_series.split(",")
                    if dicom_series in series_list:
                        seriesOk = True  # series ok
                else:
                    seriesOk = True  # by default, if the value is missing, consider it good

            LOG_TYPE = "SERIES_OK"
            LOGG = str(seriesOk)
            log_lines.append(
                LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)

            if seriesOk:

                # Find the name of the series
                serieDescription = "unnamedSeries"
                if hasattr(dataset, 'SeriesDescription') or hasattr(dataset, 'Series Description'):
                    serieDescriptionChallenger = dataset.SeriesDescription
                    if len(serieDescriptionChallenger) > 0:
                        serieDescription = serieDescriptionChallenger
                if (serieDescription == "unnamedSeries" and (
                        hasattr(dataset, 'SeriesNumber') or hasattr(dataset, 'Series Number'))):
                    serieDescriptionChallenger = str(dataset.SeriesNumber)
                    if len(serieDescriptionChallenger) > 0:
                        serieDescription = serieDescriptionChallenger

                LOG_TYPE = "SERIES_DESCRIPTION"
                LOGG = str(serieDescription)
                log_lines.append(
                    LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)

                # Build the final destination folder with the name of the series
                finalDestinationFolder = destination_folder + "\\" + days_elapsed + "-" + to_pretty_string(
                    studyDescription) + "\\" + to_pretty_string(serieDescription) + "\\"
                finalDestinationFolder = finalDestinationFolder.replace("\\", "/")

                # Anonymisation Tags DICOM
                # dataset.InstitutionName = ''
                dataset.ReferringPhysicianIDSequence = []
                dataset.PhysiciansOfRecord = ''
                dataset.PhysiciansOfRecordIDSequence = []
                dataset.PerformingPhysicianName = ''
                dataset.PerformingPhysicianIDSequence = []
                dataset.NameOfPhysicianReadingStudy = ''
                dataset.PhysicianReadingStudyIDSequence = []
                dataset.PatientBirthDate = d1
                dataset.PatientInsurancePlanCodeSequence = []
                dataset.PatientPrimaryLanguageCodeSeq = []
                dataset.OtherPatientIDs = ''
                dataset.OtherPatientNames = ''
                dataset.PatientBirthName = ''
                dataset.CountryOfResidence = ''
                dataset.RegionOfResidence = ''
                dataset.PatientTelephoneNumbers = ''
                dataset.CurrentPatientLocation = ''
                dataset.PatientInstitutionResidence = ''
                dataset.OtherPatientIDsSequence = []
                dataset.StudyDate = d1
                dataset.ContentDate = d1
                dataset.OverlayDate = ''
                dataset.CurveDate = ''
                dataset.OverlayTime = ''
                dataset.CurveTime = ''
                dataset.InstitutionAddress = ''
                dataset.ReferringPhysicianName = ''
                dataset.ReferringPhysicianAddress = ''
                dataset.ReferringPhysicianTelephoneNumber = ''
                dataset.InstitutionalDepartmentName = ''
                dataset.OperatorsName = ''
                dataset.StudyID = ''
                dataset.PersonName = ''
                dataset.PatientAddress = ''
                dataset.PatientMotherBirthName = ''
                dataset.PatientName = ''
                dataset.PatientID = ano_patient_ipp
                dataset.IssuerOfPatientID = ''
                dataset.PatientBirthTime = ''
                dataset.ReferringPhysicianName = ''
                dataset.NameOfPhysiciansReadingStudy = ''
                dataset.InstitutionalDepartmentName = ''
                dataset.ProtocolName = ''
                try:
                    dataset[0x0400, 0x0561].value = []
                except:
                    pass

                # Épargner des champs DICOM
                list_champs_a_epargner = [[0x7053, 0x1000], [0x7053, 0x1009]]
                dict_champ_values = {}
                # extract
                for champ in list_champs_a_epargner:
                    try:
                        dict_champ_values[str(hex(champ[0])) + str(hex(champ[1]))] = dataset[champ[0], champ[1]]
                    except:
                        print("Tag : " + str(hex(champ[0])) + str(hex(champ[1])) + " not Found")

                dataset.remove_private_tags()

                # restaure
                for champ in list_champs_a_epargner:
                    try:
                        dataset[champ[0], champ[1]] = dict_champ_values[str(hex(champ[0])) + str(hex(champ[1]))]
                    except:
                        print("Fail restoring Tag : [" + str(hex(champ[0])) + "," + str(hex(champ[1])) + "]")

                # If it is an UltraSound, remove part of the image
                if dataset.Modality == "US":
                    if dataset.file_meta.TransferSyntaxUID.is_compressed:
                        dataset.decompress()
                    dataset.pixel_data_handlers = [gdcm_handler, pillow_handler]
                    data = dataset.pixel_array
                    xmin = dataset.SequenceOfUltrasoundRegions[0].RegionLocationMinX0
                    xmax = dataset.SequenceOfUltrasoundRegions[0].RegionLocationMaxX1

                    ymin = dataset.SequenceOfUltrasoundRegions[0].RegionLocationMinY0
                    ymax = dataset.SequenceOfUltrasoundRegions[0].RegionLocationMaxY1
                    ywidth = ymax - xmin
                    xwidth = xmax - ymin

                    dataset.Rows = ywidth
                    dataset.Columns = xwidth
                    data2 = data[xmin:ymax, ymin:xmax, :]
                    dataset.PixelData = data2.tobytes()

                # Create the necessary folders if needed
                if not os.path.exists(os.path.dirname(finalDestinationFolder)):
                    LOG_TYPE = "FILE_FOLDER"
                    try:
                        os.makedirs(os.path.dirname(finalDestinationFolder))
                        LOGG = "FAIL"
                    except:
                        LOGG = "OK"
                    log_lines.append(
                        LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)

                # Save the modified DICOM dataset
                dicomFileName = original_file
                tokens = dicomFileName.split("/")
                dicomFileName = tokens[len(tokens) - 1]
                tokens = dicomFileName.split("\\")
                dicomFileName = tokens[len(tokens) - 1]

                LOG_TYPE = "FILE_SAVE"
                LOGG = finalDestinationFolder + dicomFileName
                log_lines.append(
                    LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)

                dataset.save_as(finalDestinationFolder + dicomFileName)
            else:
                LOG_TYPE = "ERROR_FILTER"
                LOGG = "Wrong series, filter series = " + str(ano_series) + ", dicom series = " + str(
                    series_list)
                log_lines.append(
                    LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)
        else:
            LOG_TYPE = "ERROR_FILTER"
            LOGG = "Unnamed Study"
            log_lines.append(
                LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)
    except:
        LOG_TYPE = "PYTHON_ERROR"
        LOGG = str(sys.exc_info())
        log_lines.append(LOG_TYPE + SEPARATOR + ano_patient_ipp + " - " + original_file + SEPARATOR + LOGG + ENDER)
        # quit()
        pass

    return log_lines


#######################
#######################
# BEGINNING OF SCRIPT
#######################
#######################

if __name__ == "__main__":

    # Inputs:
    # - the csv file with the columns [originalFile, destinationFolder, daysElapsed]
    # - string with all the modalities

    files = os.listdir(".")
    latest = 0
    for f in files:
        f = f.split("_")
        if f[0] == "imagesData":
            f = f[1].split(".")
            if int(f[0]) > latest:
                latest = int(f[0])

    imagesDataFile = "imagesData_" + str(latest) + ".csv"

    latest = 0
    for f in files:
        f = f.split("_")
        if f[0] == "logErrors":
            f = f[1].split(".")
            if int(f[0]) > latest:
                latest = int(f[0])

    fileNameLogErrors = "logErrors_" + str(latest) + ".csv"

    images_to_anonymize = []
    with open(imagesDataFile) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        for row in csv_reader:
            patient_ipp_anonymized = row[1]
            originalFile = row[2]
            destinationFolder = row[3]
            daysElapsed = row[4]
            series = row[5]
            series = series.replace('XXNOSERIESXX', '').strip()
            if 'originalFile' not in originalFile:
                images_to_anonymize.append([patient_ipp_anonymized, originalFile, destinationFolder, daysElapsed, series])

    print('images_to_anonymize = ', len(images_to_anonymize))
    print('fileNameLogErrors = ', fileNameLogErrors)

    # Parallel anonymisation
    num_cores = -1
    errorsFile = open(fileNameLogErrors, 'w+')
    errorsFile.write("LOG_TYPE;ipp;LOG\n")
    # backend = loky: used by default, can induce some communication and memory overhead when exchanging input and output data with the worker Python processes.
    #			multiprocessing: previous process-based backend based on multiprocessing.Pool. Less robust than loky.
    for log_lines_per_image in Parallel(n_jobs=num_cores, backend='loky')(
            delayed(anonymise_dicom)(image_to_anonymize_data) for image_to_anonymize_data in images_to_anonymize):
        for log_line in log_lines_per_image:
            errorsFile.write(log_line)

    errorsFile.close()
