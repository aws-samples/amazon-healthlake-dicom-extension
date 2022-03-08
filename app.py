#!/usr/bin/env python3

from aws_cdk import core

# from dicom_ahl.dicom_ahl_stack import DicomAhlStack
from dicom_ahl.study_stack import StudyProcessorStack

app = core.App()
StudyProcessorStack(app, "dicom-ahl")

app.synth()
