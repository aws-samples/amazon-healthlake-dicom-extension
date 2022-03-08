

# DICOM attributes to map to elements of the FHIR resource 
#     DICOM tags are collapsed into a single key for easy lookup
dicom_attribute_map = { 
   '(0020,000D)':{'key0':'0020', 'key1':'000D', 'description':'(0020,000D) Study Instance UID', 'fhir_element':'ImagingStudy.identifier','element_prefix':''},
   '(0020,000E)':{'key0':'0020', 'key1':'000E', 'description':'(0020,000E) UI SeriesInstanceUID', 'fhir_element':'ImagingStudy.series.uid','element_prefix':''},
   '(0008,0018)':{'key0':'0008', 'key1':'0018', 'description':'(0008,0018) UI SOPInstanceUID', 'fhir_element':'ImagingStudy.series.instance.uid','element_prefix':''},
   '(0010,0020)':{'key0':'0010', 'key1':'0020', 'description':'(0010,0020) Patient ID', 'fhir_element':'ImagingStudy.subject','element_prefix':'Patient/'},
   '(0008,0020)':{'key0':'0008', 'key1':'0020', 'description':'(0008,0020) DA StudyDate', 'fhir_element':'ImagingStudy.started','element_prefix':''},
   '(0008,0060)':{'key0':'0008', 'key1':'0060', 'description':'(0008,0060) CS Modality', 'fhir_element':'ImagingStudy.series.modality', 'element_prefix':''},
   '(0018,0015)':{'key0':'0018', 'key1':'0015', 'description':'(0018,0015) CS BodyPartExamined', 'fhir_element':'ImagingStudy.series.bodySite', 'element_prefix':''}
}