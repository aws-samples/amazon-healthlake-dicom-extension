import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="dicom_ahl",
    version="0.0.2",

    description="DICOM HealthLake CDK Python app",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Andy Schuetz",

    package_dir={"": "dicom_ahl"},
    packages=setuptools.find_packages(where="dicom_ahl"),

    install_requires=[
        "aws-cdk.core==1.124.0",
        "aws-cdk.aws_iam==1.124.0",
        "aws-cdk.aws_sqs==1.124.0",
        "aws-cdk.aws_sns==1.124.0",
        "aws-cdk.aws_sns_subscriptions==1.124.0",
        "aws-cdk.aws_s3==1.124.0",
    ],

    python_requires=">=3.8",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
