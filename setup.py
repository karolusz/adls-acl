from setuptools import setup, find_packages

# readme = open("README.md", encoding="utf-8").read()
# version = open("aam/VERSION", encoding="utf-8").read().strip()

setup(
    name="adls_acl",
    version="0.0.1",
    url="https://github.com/karolusz/adls-acl",
    author="Karol Luszczek",
    author_email="karol.luszczek@reinsight.se",
    description="A small tool for managing ADLS Access Control List.",
    # long_description=readme,
    # long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["yamale", "azure-storage-file-datalake", "azure-identity"],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": ["adls-acl=adls_acl.cli:main"],
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
    ],
)
