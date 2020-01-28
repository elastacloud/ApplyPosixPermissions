using ApplyPosixPermissions.Interfaces;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.Wrappers
{
    internal class AzureDataLakeStorageWrapper : IAzureDataLakeStorageWrapper
    {
        private readonly IAzureDataLakeStorage _storage;

        public AzureDataLakeStorageWrapper(IAzureDataLakeStorage storage)
        {
            _storage = storage;
        }

        public Task CreateFilesystemAsync(string filesystem)
        {
            return _storage.CreateFilesystemAsync(filesystem);
        }

        public Task CreateFolderAsync(string folderPath, string dummyFileName = null,
            CancellationToken cancellationToken = default)
        {
            return _storage.CreateFolderAsync(folderPath, dummyFileName, cancellationToken);
        }

        public Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken = default)
        {
            return _storage.ExistsAsync(fullPath, cancellationToken);
        }

        public Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false,
            CancellationToken cancellationToken = default)
        {
            return _storage.GetAccessControlAsync(fullPath, getUpn, cancellationToken);
        }

        public Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null,
            CancellationToken cancellationToken = default)
        {
            return _storage.ListAsync(options, cancellationToken);
        }

        public Task<IReadOnlyCollection<Filesystem>> ListFilesystemsAsync(CancellationToken cancellationToken = default)
        {
            return _storage.ListFilesystemsAsync(cancellationToken);
        }

        public Task SetAccessControlAsync(string fullPath, AccessControl accessControl,
            CancellationToken cancellationToken = default)
        {
            return _storage.SetAccessControlAsync(fullPath, accessControl, cancellationToken);
        }
    }
}