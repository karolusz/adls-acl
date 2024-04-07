Azure DataLake Storage (ADLS)- Access Control List (ACL) Manager
=================

A small CLI tool for managing Azure DataLake Storage (ADLS) [Access Control Lists (ACLs)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control) for containers and directories.

It allows you to take control of your ADLS account's directory structure and ACLs as Infrastructure as Code through the use of YAML configuration files.

[![Tests](https://github.com/karolusz/adls-acl/actions/workflows/tests.yml/badge.svg)](https://github.com/karolusz/adls-acl/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/Adls-acl.svg)](https://pypi.org/project/adls-acl/)

Requirements
------------
* Python 3.12+
* Yamale
* azure-identity
* azure-storage-file-datalake

Install
-------
### pip
```bash
$ pip install adls-acl 
```


Usage
-----
### Command line
`adls-acl` can be run from the command line to create directories and set desired ACLs in the Azure Storage Account Gen v2 as defined in a user supplied YAML files. 

Containers and directories defined in the config file, but not present in the storage account, will be created during `adls-acl` run. The ACLs for existing directories in the storage account, will be overwritten with those specified in the input config file. Future releases shall enable alternative behaviors. For that reason, the current version of `adls-acl` is best for green field deployments.

The Azure Identity client (Python SDK) is used for authenticating to Microsoft Entra ID (former Azure AD). It currently uses `DefaultAzureCredential` ([MS DOCS: DefaultCredential](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential)), which enables authentication with multitude of methods (in the future a user will be able to target a specific authentication mechanism via a CLI option in `adls-acl` for better control).

Usage:

```
Usage: adls-acl [OPTIONS] COMMAND [ARGS]...

Options:
  --debug          Enable debug messages.
  --silent         Suppress logs to stdout.
  --log-file TEXT  Redirect logs to a file.
  --help           Show this message and exit.

Commands:
  set-acl  Read and set direcotry structure and ACLs from a YAML file.
```

#### `set-acl` command
```
Usage: adls-acl set-acl [OPTIONS] FILE

  Read and set direcotry structure and ACLs from a YAML file.

Options:
  --help  Show this message and exit.
```

To set acls from an input file `test.yml` the shell command would look like:
```bash
adls-acl set-acl test.yml
```
### Input file

The YAML schema reference for the input files. Each input file represents a desired directory structure and ACLs for a single Azure Storage account. 

##### Input File Example
Example of an input file for a fictitious storage account. All elements of the schema are explained in the following sections.

```yaml
account: testaccount
containers: 
  - name: testcontainer1 
    acls:
      - oid: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        type: "user"
        acl: r-x
      - oid: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
        type: "user"
        acl: --x
    folders:
      - name: directory_a
        acls:
          - oid: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
            type: "group"
            acl: rwx
            scope: default
        folders:
          - name: subdir_a 
            acls:
              - oid: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
                type: "user"
                acl: --x
      - name: directory_b
        acls:
          - oid: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
            type: "group"
            acl: rwx
            scope: default
```

The above input would create the following directory structure in the storage account `testaccount`:

```
testcontainer1 (storage container)
root/
|
├── directory_a/
|   ├── subdir_a/
|
├── directory_b/

```
- Multiple containers can be specified in the config file.
- ACLs on the container are applied on the container's root directory.
- Subdirectories can be nested to create a desired directory hierarchy.


#### Account - definition
```yaml
account: string # Required. The name of the Azure storage acccount.
containers: [ folder ] # list of containers in the account.
```
`account` string. Required.
Azure Storage Account name as in: `https://<account>.dfs.core.windows.net/`

`container`  [folder](#folder--definition),
A list of objects describing directories and their ACLS. In the context of the container it defines container's name, ACLs on the container root and subdirectories.


#### Folder - definition
```yaml
name: string # Required. Direcotry name.
acls: [ acl ] # A list of ACLs to set on the directory.
folders: [ folder ] # A list of subdirectory objects.
```
`name` string. Required.
A name of a directory.

`acls` [acl](#acl---definition)
A list of ACLs to set on the directory.

`folders` [folder](#folder---definition)
A list of objects describing subdirectories.

#### Acl - definition
```yaml
oid: string. # Required. Security principal Object ID in Microsoft Entra ID.
type: string # Required. Security principal type.
acl: string 
scope: string. 
```
`oid` string. Required.
Object ID of the principal (user/group/managed identity/service principal) in Microsoft Entra (former Azure Active Directory).

`type` string. Required.
Type of the service principal. Allowed values: `user` (for users, service principals, and managed identities), `group` (for Entra ID groups), `other` (for all other users ACLs), `mask` (for setting masks on directories)

`acl` string. Required.
A string defining desired permissions in the short form. [MS DOCS: ADLS ACLs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control)
e.g.:
`r--` for read-only permissions

`scope` string. Optional
If set to `default` it will set the specified ACLs as `default ACLs` [MS DOCS: Types of ACLs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control#types-of-acls). If not present, ACLs will be set as `access ACLs`.

#### Special ACLs

`adls-acl` also allows for managing ACLs for owning user, owning group, all other users, as well as setting masks. Examples of how to specify each of the above, in the `adls-acl` YAML input file (as [acl](#acl---definition) block) are provided below:

- owning user
```yaml
oid: ""
type: user
acl: ---
```
- owning group
```yaml
oid: ""
type: group
acl: ---
```
- other users
```yaml
oid: ""
type: other
acl: ---
```
- masks
```yaml
oid: ""
type: mask
acl: ---
```
All of the above can be set as default ACLs by adding `scope: default` parameter.


### Default ACLs

The default ACLs defined or set on the higher level directories are pushed down to subdirectories specified in the input file.
They will not be set on any files that had existed in the directories prior to the execution of `adls-acl`.
Moreover, any subdirectories that exist in the account but are not specified in the input file remain untouched by `adls-acl`.

Future releases will allow for more control over this behaviour (i.e, updating default ACLs on all files created prior to the change of ACLs).

