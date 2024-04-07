from setuptools import setup, find_packages

readme = open("README.md", encoding="utf-8").read()

setup(
    name="adls_acl",
    version="0.0.4",
    url="https://github.com/karolusz/adls-acl",
    author="Karol Luszczek",
    author_email="karol.luszczek@reinsight.se",
    description="A small tool for managing Azure DataLake Store (ADLS) Access Control Lists (ACLs).",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "yamale",
        "azure-storage-file-datalake",
        "azure-identity",
        "click",
    ],
    python_requires=">=3.12",
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-mock", "bumpver"]},
    entry_points={
        "console_scripts": ["adls-acl=adls_acl.cli:cli"],
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
