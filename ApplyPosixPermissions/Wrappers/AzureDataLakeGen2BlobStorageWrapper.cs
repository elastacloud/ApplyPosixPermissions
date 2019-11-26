using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ApplyPosixPermissions.Interfaces;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;

namespace ApplyPosixPermissions.Wrappers
{
    internal class AzureDataLakeGen2BlobStorageWrapper : IAzureDataLakeGen2BlobStorageWrapper
    {
        private readonly IAzureDataLakeGen2BlobStorage _storage;

        public AzureDataLakeGen2BlobStorageWrapper(IAzureDataLakeGen2BlobStorage storage)
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

        public Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false)
        {
            return _storage.GetAccessControlAsync(fullPath, getUpn);
        }

        public Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null,
            CancellationToken cancellationToken = default)
        {
            return _storage.ListAsync(options, cancellationToken);
        }

        public Task<IEnumerable<string>> ListFilesystemsAsync()
        {
            return _storage.ListFilesystemsAsync();
        }

        public Task SetAccessControlAsync(string fullPath, AccessControl accessControl)
        {
            return _storage.SetAccessControlAsync(fullPath, accessControl);
        }
    }
}