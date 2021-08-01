import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyscd",                     # This is the name of the package
    version="1.0.2",                        
    author="Ankan Mukherjee",                     # Full name of the author
    description="This is a package that allows you to implement a change data capture using SCD type 2",
    long_description="This is a package that allows you to implement a change data capture using SCD type 2",      
    long_description_content_type="text/markdown",
    url = "https://github.com/Ankan1991/pyscd",
    packages=setuptools.find_packages(),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    python_requires='>=3.6',                # Minimum version requirement of the package
    py_modules=["pyscd"],             # Name of the python package
    package_dir={'':'pyscd/src'},     # Directory of the source code of the package
    install_requires=[]                     # Install other dependencies if any
)