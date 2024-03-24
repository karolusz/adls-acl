from setuptools import setup, find_packages

readme = open("README.md", encoding="utf-8").read()
license = open("LICENSE", encoding="utf-8").read()
# version = open("yamale/VERSION", encoding="utf-8").read().strip()

setup(
    name="yamale",
    version="0.0.1",
    url="https://github.com/23andMe/Yamale",
    author="Bo Lopker",
    author_email="blopker@23andme.com",
    description="A schema and validator for YAML.",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["pyyaml"],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": ["yamale=yamale.command_line:main"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
