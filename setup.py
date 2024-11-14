from setuptools import setup, find_packages


VERSION = "0.0.13"


setup(
    name="schemon-python-client",
    version=VERSION,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    license="Apache License 2.0",
    license_files=["LICENSE"],  # Specify the license file
    install_requires=[
        "pyspark==3.5.0",
        "cryptography==43.0.3",
        "delta-spark==3.2.0",
        "boto3==1.35.18",
        "mysql-connector-python==9.1.0",
        "schemon_python_logger",
        "openpyxl==3.1.5",
    ],
    entry_points={},
    python_requires=">=3.9",
    include_package_data=True,  # Include package data specified in MANIFEST.in
    package_data={},
    exclude_package_data={},
)
