""" Lambda function for converting DICOM study payload to FHIR Imaging Study Resource """

import os
import json
from io import BytesIO
import logging
import sys
from typing import List, Dict
import pprint
from copy import deepcopy
from functools import reduce
from operator import getitem

import boto3
from botocore.config import Config
from botocore.awsrequest import AWSRequest
from botocore.endpoint import URLLib3Session
from botocore.auth import SigV4Auth
import pydicom as pyd
from pydicom.errors import InvalidDicomError

from resources import dicom_attribute_map

logger = logging.getLogger()
logger_levels = {'CRITICAL':logging.CRITICAL, 'ERROR':logging.ERROR, 
                  'WARNING':logging.WARNING, 'INFO':logging.INFO, 
                  'DEBUG':logging.DEBUG}
try:
   log_input = str(os.environ['LOG_LEVEL']).upper()
   logger.setLevel(logger_levels[log_input])
   logger.info('Set logging level to {}'.format(logger.level))
except( KeyError, ValueError):
   logger.setLevel(logging.INFO)
   logger.error('Unrecognized log level configuration.  Defaulting to INFO.')

# Initialize series level UIDs
study_instance_uid = None
series_instance_uid = None

region = os.environ['REGION']
end_byte = int(os.environ['SOP_BYTES_READ'])
template_bucket = os.environ['TEMPLATE_BUCKET']             
template_key = os.environ['TEMPLATE_KEY'] 
template_map_key = os.environ['TEMPLATE_MAP_KEY'] 

host = os.environ['HEALTHLAKE_HOST']                          
endpoint_base = os.environ['HEALTHLAKE_ENDPOINT']     

dicom_bad_queue = os.environ['BAD_QUEUE']

my_config = Config(region_name=region)
s3 = boto3.client('s3', config=my_config)
sqs = boto3.client('sqs', config=my_config)

def set_nested_item(dataDict, mapList, val):
   """ Set item in nested dictionary """
   reduce(getitem, mapList[:-1], dataDict)[mapList[-1]] = val
   return dataDict

class Instance:
   """ Utility class to represent SOP Instance """
   def __init__(self,bucket: str, key: str, end_byte=end_byte) -> None:
      self.end_byte = end_byte # Max number of bytes to stream off S3.  Pydicom will stop before metadata
      self.bucket = bucket
      self.key = key
      self.dcm = None     

      self.study_uid = ''
      self.series_uid = ''
      self.series_number = None  # (0020,000E) Series Number
      self.instance_uid = ''
      self.patient_id = ''
      
      # Construct s3 URL
      self.s3_url = 's3://{}/{}'.format(bucket,key)

      self.read_instance()

   def __repr__(self) -> None:
      """ bare bones repr """
      return str(self.__dict__)

   def read_tags_from_s3(self) -> pyd.Dataset:
      """ Read SOP instance metadata from S3 """

      try:
         # get_object will return StreamingBody file-like object, 
         #  limit bytes streamed as defense against out of memory
         logger.debug('Attempting to open StreamingBody file like from S3 bucket: {}, key: {}'.format(self.bucket,self.key))
         resp = s3.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(0, self.end_byte))

         # Read stream as DICOM, stopping before the pixel data
         dcm = pyd.dcmread(BytesIO(resp['Body'].read()),stop_before_pixels=True)
         logger.debug('DICOM meta-data: {}'.format(dcm))

      except(InvalidDicomError, TypeError):
         logger.error('Failed to read DICOM file {}'.format(key))

      self.dcm = dcm

   def read_instance(self) -> None:
      """ Read SOP instance from S3 """

      self.read_tags_from_s3()

      self.study_uid = self.dcm['0020','000D'].value       # (0020,000D) Study Instance UID
      self.series_uid = self.dcm['0020','000E'].value      # (0020,000E) UI SeriesInstanceUID
      self.instance_uid = self.dcm['0008','0018'].value    # (0008,0018) UI SOPInstanceUID
      self.patient_id = self.dcm['0010','0020'].value      # (0010,0020) Patient ID, Type = Long String (LO)
      self.study_date = self.dcm['0008','0020'].value
      self.instance_number = self.dcm['0020','0013'].value # (0020,0013) IS InstanceNumber
      self.series_number = self.dcm['0020', '0011'].value  # (0020,0011) Series Number
      self.modality = self.dcm['0008', '0060'].value       # (0008,0060) Modality

class Study:
   """ Utility class for organizing SOP instances into FHIR ImagingStudy resource data structure """
   
   def __init__(self, bucket: str, instance_keys: List[str]) -> None:
      self.bucket = bucket
      self.instance_keys = instance_keys
      self.uid = '' # uid
      self.instance = None          # Encapsulate one instance object
      self.series = {}              # Dict where values are lists of Instance objects {'series_uid':[inst_a, inst_b, ...]}
      self.series_fhir = {}         # Dict with structure of FHIR series and custom resources for S3 paths
      self.imagingstudy_fhir = {}   # Dict with structure of imagingstudy.  Can be written as JSON FHIR resource.  This is payload that is POST'ed to AHL

      self.load()

   def __repr__(self) -> str:
      # TODO, make better repr
      return str(self.__dict__)

   def add_instance(self, inst: Instance) -> None:

      if inst.series_uid in self.series.keys():
         # Series already exists in study object, so append sop instance
         self.series[inst.series_uid].append(inst)

      else:
         # New series, add to study
         #  inst.series_uid is a string, and that's the key into a dict, where values are lists of Instance objs
         self.series[inst.series_uid] = [inst]

   def load(self) -> None:
      """ load instances supplied in instance_keys """

      for key in self.instance_keys:

         sop = Instance(self.bucket, key)

         if self.uid == '':                           # Study UID not yet set, so assume first SOP Instance has correct UID
            self.uid = sop.study_uid
            self.add_instance(sop)
            logger.info('Set Study UID to {} and added {} to study.'.format(self.uid,key))
         elif self.uid == sop.study_uid:
            self.add_instance(sop)    
            logger.info('Added {} to study.'.format(key))
         
         elif self.uid != sop.study_uid:
               # Current SOP instance doesn't have same study UID, submit to DLQ
               logger.error('SOP instance {} has different Study UID than Study (first SOP instance processed'.format(key))
               self.to_dl_queue(key)
         else:
            self.add_instance(sop)    
            logger.info('Added {} to study.'.format(key))

         if self.instance == None:
            # Encapsulate one Instance object in Study object.  TODO: More rigorous treatment of meta-data
            self.instance = sop

   def _instance_to_FHIR(self,instance: str) -> dict:
      """   Create dict in format of FHIR custom Intance resource """
      # self: instance of Study class
      # instance: key for SOP Instance on S3
      

      obj = {'uid': instance.instance_uid,
            'sopClass': {
               'system':  "urn:ietf:rfc:3986",
               'code': "urn:oid:1.2.840.10008.5.1.4.1.1.2" },
            'number': int(instance.instance_number),                    # Instance Number (0020,0013)
            'extension': [
               {'url': "http://healthlake.amazonaws.com/s3-uri/",       # TODO: This should be published by AHL or other server
                'valueUri': instance.s3_url}]
            }

      return obj

   def _series_to_FHIR(self,series_key: str) -> dict:
      """ Convert series to FHIR resource substring string for ImagingStudy """
      # series_key is string key into self.series dict
      #
      # series attribute is dict, where values are lists of instance objects.  Concatenate to string of FHIR resource.
      # WARNING: Assumes its accurate to take metadata from the first SOP instance in the series.

      sops = self.series[series_key] # returns list of sop instance objects

      series_out = { 'uid': str(sops[0].series_uid),                        
                      'number': int(sops[0].series_number),                 
                      'modality': {
                        'system': "http://dicom.nema.org/resources/ontology/DCM",
                        'code': sops[0].modality,                              
                        },
                      'description': " ",                                      # TODO sops[0].description, # (0008,103E) Series Description
                      'numberOfInstances': len(sops),                          # Number of instances received
                      'bodySite': {
                        'system': "http://snomed.info/sct",
                        'code': "123037004",                                   # (0018,0015) Body Part Examined, put blank as place holder
                        'display': "Body structure"                               
                        },
                     'endpoint': [{
                                  "reference": "Endpoint/example-wadors"      # TODO: Add WADORS
                                  }],
                     'instance': []
                   }

      for sop in sops:

         series_out['instance'].append(self._instance_to_FHIR(sop))

      return series_out

   def _series_to_fhir(self) -> None:
      """ Write all series, consisting of all SOP instances to FHIR formatted dict """
      
      series_keys = self.series.keys()
      series_fhir = []                       # FHIR series resource is a list type

      for key in series_keys:

         series_fhir.append(self._series_to_FHIR(key))

      print(pprint.pformat(series_fhir))

      self.series_fhir = series_fhir

   def _template_from_s3(self, bucket, template_key, map_key):
      """ Read FHIR template and corresponding parameter map from S3 """

      template_in = s3.get_object(Bucket=bucket, Key=template_key)
      template = json.load(BytesIO(template_in['Body'].read()))
               
      template_in = s3.get_object(Bucket=bucket, Key=map_key)
      template_map = json.load(BytesIO(template_in['Body'].read()))

      return template, template_map

   def _update_fhir_template(self, template, template_map) -> None:
      """ Update FHIR template with study meta data """

      fhir_map = {}
      for obj in template_map['template_map']:
         fhir_map[obj['key']] = obj['location']

      updates = []
      for tag_to_map in template_map['tags_to_fhir']:
         # For each DICOM tag that is configured to be mapped from the original SOP instance
         #  1) look up value from meta-data ingested
         #  2) Find location in template
         #  3) Write value to corresponding element in the FHIR resource

         tmp_map = dicom_attribute_map[tag_to_map]
         tmp_tag0 = tmp_map['key0'] 
         tmp_tag1 = tmp_map['key1']

         # Read value from DICOM meta-data
         try:
            if tmp_map['element_prefix']:
               tag_value = tmp_map['element_prefix'] + self.instance.dcm[tmp_tag0, tmp_tag1].value      # TODO: create getter so Study can be addressed this way
            else:
               tag_value = self.instance.dcm[tmp_tag0, tmp_tag1].value
         except KeyError:
            logger.error('Error: Failed to find ({},{}) in SOP instance'.format(tmp_tag0,tmp_tag1))         

         # Find FHIR element location in template
         template_loc = fhir_map[tag_to_map]

         # Add to list of updates to the template
         updates.append( (template_loc, tag_value) )

      logger.debug(updates)

      resource = deepcopy(template)                            
      # Write DICOM meta-data values to FHIR resource elements 
      for map_list, val in updates:
         try:
            resource = set_nested_item(resource, map_list, val)
         except(KeyError, IndexError):
            logger.error('Error: Failed to write {} to FHIR resource template'.format(map_list))

      resource['series'] = self.series_fhir

      # # Add total number of instances within ImagingStudy if attribute in template
      # if 'numberOfInstances' in resource.keys():
      #    num = 0
      #    for ser in self.series_fhir:
      #       num += len(ser['instance'])
      #    resource['numberOfInstances'] = num

      self.imagingstudy_fhir = resource

      # Apply transformations to the base FHIR resource
      self._transform_fhir()

      # At this point the FHIR resourece is ready to be POSTed to AHL
      return

   def _transform_fhir(self) -> None:
      """ Apply transformations to the full FHIR json resource 

          Some elements of the FHIR resource cannot be written until the full object is 
          assembled (such as numberOfInstances).  This function applys transforms to update 
          the base FHIR resource constructed from the template and initial transfer of 
          meta-data from the DICOM SOP instances. 

          If required by your use-case add additional transformers here.

      """

      resource = self.imagingstudy_fhir

      # Insert total number of instances within ImagingStudy if attribute in template
      if 'numberOfInstances' in resource.keys():
         num = 0
         for ser in self.series_fhir:
            num += len(ser['instance'])
         resource['numberOfInstances'] = num

      # Add additional transformer logic here

      self.imagingstudy_fhir = resource


   def imagingstudy(self, template_bucket, template_key, map_key) -> None:
      """ Construct ImagingStudy FHIR resoruce """
      
      # Construct nested dict with all series and sop instances
      self._series_to_fhir()

      # Pull FHIR template from S3
      template, template_map = self._template_from_s3(template_bucket, template_key, map_key)

      # Build FHIR json
      self._update_fhir_template(template, template_map)

      # Apply transformations to complete FHIR json
      self._transform_fhir()

   def post_to_healthlake(self, host, region, endpoint_base):
      """ Helper to perform POST to Amazon Healthlake """
      # host:
      # region: 
      # endpoint: 

      resource = self.imagingstudy_fhir
      logger.info('Preparing to POST this resource to Healthlake: {}'.format(resource))
      
      method = 'POST'
      service = 'healthlake'
      endpoint = endpoint_base + resource['resourceType']

      headers = {'Host': host, 'Content-Type': 'application/json'}

      # HealthLake does not yet have boto3 support for CRUD APIs, so we create an API call with signature
      request = AWSRequest(method=method, url=endpoint, data=json.dumps(resource), headers=headers)
      SigV4Auth(boto3.Session().get_credentials(), service, region).add_auth(request)
      session = URLLib3Session()
      r = session.send(request.prepare())

      return r

   def to_dl_queue(self, inst_key):
      # Submit SOP instance to DLQ
   
      message_body = json.dumps({'bucket': self.bucket,
                                 'instances':list(inst_key)})                

      response = sqs.send_message(QueueUrl=dicom_bad_queue,                    # TODO: Taken from outer scope.  Clean up.
                                     MessageBody=message_body)

      return response

#
#  Lambda Event Handler That Consumes from SQS Queue
#
def sqs_lambda_handler(event, context):
   """ Lambda consumer for SQS """

   logger.info('Received event --- {}'.format(event))

   for payload in event['Records']: # One lambda execution for a batch of messages
     
      # event comes in as dict --- payload = json.loads(event['body'])
      logger.info('Received payload --- {}'.format(payload))
        
      # body comes in as json string
      body = json.loads(payload['body'])

      print(body)
      
      if type(body) is dict:
         bucket = body['bucket']
         instances = body['instances']
      elif type(body) is list and len(body) == 1:
         bucket = body[0]['bucket']
         instances = body[0]['instances']
      else:
         ValueError('Unrecognized payload body')
    
      logger.info('Received bucket {}, and instances {}.'.format(bucket,instances))

      # Construct Study object from sop instances received in payload
      astudy = Study(bucket=bucket,instance_keys=instances)

      # Construct FHIR resource ImagingStudy
      astudy.imagingstudy(template_bucket, template_key, template_map_key)
      
      r = astudy.post_to_healthlake(host, region, endpoint_base)
      
      logger.debug(r.text)
      logger.info('POST to AHL code: {}'.format(r.status_code))
      logger.debug('POST to AHL text: {}'.format(r.text))

      return {
         'statusCode': r.status_code,
         'headers': {'Content-Type': 'text/plain'},
         'body': 'POST to HealthLake returned  {},{}\n'.format(r.status_code, r.text)
      }
