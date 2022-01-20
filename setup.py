import setuptools

setuptools.setup(
    name="sidspipeline",
    version="1.0.0",
    author="Ioan Ferencik",
    author_email="ioan.ferencik@undp.org",
    description="A GDAL based geospatial data processing pipeline.",
    #long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.solargis.com/sat/idownloader",
    packages=setuptools.find_packages(),

    license='LGPL',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'azure-storage-blob',
        'gdal>=3',
        'numpy',
        'pygeoprocessing'

    ],

    entry_points={
        'console_scripts': ['sidspipeline=sidspipline.pipeline:main'],
    },
)