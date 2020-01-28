using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.Interfaces
{
    public interface IAzureDataLakeStorageWrapper
    {
        Task CreateFilesystemAsync(string filesystem);

        Task CreateFolderAsync(string folderPath, string dummyFileName = null,
            CancellationToken cancellationToken = default);

        Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken = default);

        Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false,
            CancellationToken cancellationToken = default);

        Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null,
            CancellationToken cancellationToken = default);

        Task<IReadOnlyCollection<Filesystem>> ListFilesystemsAsync(CancellationToken cancellationToken = default);

        Task SetAccessControlAsync(string fullPath, AccessControl accessControl,
            CancellationToken cancellationToken = default);
    }
}