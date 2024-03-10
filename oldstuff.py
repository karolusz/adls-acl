def get_directory_client(
    container: str, parent_dir: str, service_client: DataLakeServiceClient
) -> DataLakeDirectoryClient:

    directory_client = service_client.get_file_system_client(
        file_system=container
    ).get_directory_client(parent_dir)

    return directory_client


def update_permission_recursively(
    client: ClientWithACLSupport,
    oid: str,
    p_type: str,
    acl: str,
    is_default_scope: bool = False,
) -> None:
    permissions = f"{p_type}:{oid}:{acl.upper()}"
    if is_default_scope:
        permissions = f"default:{permissions}"
    print(permissions)
    client.update_access_control_recursive(acl=permissions)

    return None


def update_permission_nonrecursively(
    client: ClientWithACLSupport,
    oid: str,
    p_type: str,
    acl: str,
    is_default_scope: bool = False,
) -> None:
    current_acls = client.get_access_control()["acl"]
    # Check if acl for user:type exists
    # DEV
    # for str_acl in current_acls.split(","):
    #    acl = Acl.from_str(str_acl)
    #    print(acl)
    #    input()
    acl_prefix = f"{p_type}:{oid}:"
    if is_default_scope:
        acl_prefix = f"default:{acl_prefix}"
    # pattern = rf"(?<!default:){p_type}:{re.escape(oid)}:"
    pattern = rf"(?<!default:){re.escape(acl_prefix)}"
    if re.search(pattern, current_acls) is not None:
        pattern = rf"({pattern})(.*?)($|,|\n)"
        new_acls = re.sub(pattern, rf"\1{acl.lower()}\3", current_acls)
    else:
        new_acls = f"{current_acls},{acl_prefix}{acl.lower()}"
    print(new_acls) client.set_access_control(acl=new_acls)

    return None


def update_permissions(client: ClientWithACLSupport, acl: Dict):
    if "recursive" in acl:
        if acl["recursive"] == True:
            if "acl" in acl:
                update_permission_recursively(
                    client=client, oid=acl["oid"], p_type=acl["type"], acl=acl["acl"]
                )
            if "default_acl" in acl:
                update_permission_recursively(
                    client=client,
                    oid=acl["oid"],
                    p_type=acl["type"],
                    acl=acl["default_acl"],
                    is_default_scope=True,
                )
            return None
    else:
        if "acl" in acl:
            update_permission_nonrecursively(
                client=client, oid=acl["oid"], p_type=acl["type"], acl=acl["acl"]
            )
        if "default_acl" in acl:
            print("DEFAULT ACL:,", acl)
            update_permission_nonrecursively(
                client=client,
                oid=acl["oid"],
                p_type=acl["type"],
                acl=acl["default_acl"],
                is_default_scope=True,
            )


def update_acls(client: ClientWithACLSupport, acls: List[object]):
    for acl in acls:
        update_permissions(client, acl)


def traverse_folders(fc: FileSystemClient, path, folder_config):
    if path == "":
        path = folder_config["name"]
    else:
        path = f"{path}/{folder_config['name']}"
    print(f"PATH: {path}")
    dc = fc.get_directory_client(path)
    if not dc.exists():
        dc.create_directory()

    update_acls(dc, folder_config["acls"])
    dc.close()

    if "folders" in folder_config:
        for folder in folder_config["folders"]:
            traverse_folders(fc, path, folder)

    return None


def traverse_containers(sc: DataLakeServiceClient, containers: List[object]):
    for container in containers:
        container_name = container["name"]
        # Create container if it doesn't exist
        # Get the file system client
        try:
            fc = sc.create_file_system(container_name)
        except ResourceExistsError:
            fc = sc.get_file_system_client(file_system=container_name)

        # Update acls on the root directory
        update_acls(fc._get_root_directory_client(), container["acls"])
        for folder in container["folders"]:
            traverse_folders(fc, "", folder)
        fc.close()

