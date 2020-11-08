import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cbanalysis",  # Replace with your own username
    version="0.0.1",
    author="Ri Le",
    author_email="r.le@columbia.edu",
    description="Data processing for Citi Bike data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rl2999/cb-analysis",
    py_modules=["c"],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)