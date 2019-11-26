# ApplyPosixPermissions

C# .NET Core 3.0 console application to apply [Azure Data Lake Gen 2 POSIX](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control) permissions based on a JSON configuration to manage fine grain filesystem access control. The application uses the [Storage.NET](https://github.com/aloneguid/storage) Azure Data Lake Gen 2 API.

For one or more directories defined in the JSON configuration, if the ACL's defined in the configuration do not match the ACL's defined in the Data Lake, then the Data Lake's directory and children will be updated to match the JSON's configuration.

## Example Configuration

```json
[
  {
    "path": "filesystem1",
    "upn": false,
    "recurse": true,
    "force": false,
    "acls": [
      {
        "objectType": "User",
        "identity": "00000000-0000-0000-0000-000000000000",
        "read": true,
        "write": false,
        "execute": true,
        "defaultRead": true,
        "defaultWrite": false,
        "defaultExecute": true
      },
      {
        "objectType": "User",
        "identity": "user@onmicrosoft.com",
        "read": true,
        "write": true,
        "execute": true,
        "defaultRead": true,
        "defaultWrite": true,
        "defaultExecute": true
      },
      {
        "objectType": "Group",
        "identity": "00000000-0000-0000-0000-000000000000",
        "read": false,
        "write": true,
        "execute": true,
        "defaultRead": false,
        "defaultWrite": true,
        "defaultExecute": true
      }
    ]
  },
  {
    "path": "filesystem2/directory",
    "upn": true,
    "recurse": true,
    "force": false,
    "acls": [
      {
        "objectType": "User",
        "identity": "00000000-0000-0000-0000-000000000000",
        "read": true,
        "write": false,
        "execute": true,
        "defaultRead": true,
        "defaultWrite": false,
        "defaultExecute": true
      },
      {
        "objectType": "User",
        "identity": "user@onmicrosoft.com",
        "read": true,
        "write": true,
        "execute": true,
        "defaultRead": true,
        "defaultWrite": true,
        "defaultExecute": true
      },
      {
        "objectType": "Group",
        "identity": "00000000-0000-0000-0000-000000000000",
        "read": false,
        "write": true,
        "execute": true,
        "defaultRead": false,
        "defaultWrite": true,
        "defaultExecute": true
      }
    ]
  }
]
```

### Properties
- "path": filesystem or directory to compare permissions.
- "upn": lookup user's identity by [User Principal Name](https://docs.microsoft.com/en-us/azure/active-directory/hybrid/plan-connect-userprincipalname).
- "recurse": recursively apply permissions to children.
- "force": apply permissions to a directory and children irrespective of whether the actual ACL's match the expected ACL's.
- "acls": access control list of service principals to apply against the directory and it's children.
- "objectType": supported types are "User" (for Active Directory users, applications, etc) and "Group" (for Active Directory groups).
- "identity": service principal object id (or upn for user if enabled). Please see [Get-AzADServicePrincipal](https://docs.microsoft.com/en-us/powershell/module/az.resources/get-azadserviceprincipal?view=azps-3.0.0) for more information.
- "read": read permission on directory and existing items.
- "write": write permission on directory and existing items.
- "execute": execute permission on directory and existing items.
- "defaultRead": read permission on directory and new items.
- "defaultWrite": write permission on directory and new items.
- "defaultExecute": execute permission on directory and new items.

## Getting Started
### Requirements
- [.NET Core 3.0](https://dotnet.microsoft.com/download/dotnet-core/3.0)

### Run
Create a JSON configuration file using the template provided above and run `ApplyPosixPermissions.exe --StorageAccountName StorageAccountName --SharedAccessKey StorageSharedAccessKey --ConfigurationPath JsonConfigurationFilePath --DefaultConnectionLimit 48(optional) --Expect100Continue false(optional)`.

## Build and Test
Created using Visual Studio 2019. 
Unit tests are implemented using MSTest.

## Contribute
Please feel free to raise any issues or pull requests to help improve this project.