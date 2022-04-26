import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyscd",                     
    version="1.0.6",                        
    author="Ankan Mukherjee", 
    author_email= "ankanmukherjee6@gmail.com",
    description="This is a package that allows you to implement a change data capture using SCD type 2 in Pyspark",
    long_description="This is a package that allows you to implement a change data capture using SCD type 2 in Pyspark",      
    long_description_content_type="text/markdown",
    url = "https://github.com/Ankan1991/pyscd",
    packages=setuptools.find_packages(),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    python_requires='>=3.6',                # Minimum version requirement of the package
    py_modules=["pyscd"],             # Name of the python package
    package_dir={'':'pyscd/src'},     # Directory of the source code of the package
    install_requires=[]                     # Install other dependencies if any
)
