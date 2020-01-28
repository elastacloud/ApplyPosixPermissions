using ApplyPosixPermissions.Interfaces;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.Wrappers
{
    public class RetryableAzureDataLakeStorageWrapper : IAzureDataLakeStorageWrapper
    {
        private readonly IAzureDataLakeStorageWrapper _storage;
        private const int Retries = 3;
        private const int Delay = 1000;

        public RetryableAzureDataLakeStorageWrapper(IAzureDataLakeStorage storage) : this(new AzureDataLakeStorageWrapper(storage))
        {

        }

        public RetryableAzureDataLakeStorageWrapper(IAzureDataLakeStorageWrapper storage)
        {
            _storage = storage;
        }

        public async Task CreateFilesystemAsync(string filesystem)
        {
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    await _storage.CreateFilesystemAsync(filesystem);
                    break;
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", e);
                    }

                    await Task.Delay(Delay * i);
                }
            }
        }

        public async Task CreateFolderAsync(string folderPath, string dummyFileName = null, CancellationToken cancellationToken = default)
        {
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    await _storage.CreateFolderAsync(folderPath, dummyFileName, cancellationToken);
                    break;
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", e);
                    }

                    await Task.Delay(Delay * i);
                }
            }
        }

        public async Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken = default)
        {
            Exception inner = null;
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    return await _storage.ExistsAsync(fullPath, cancellationToken);
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        inner = e;
                    }
                    else
                    {
                        await Task.Delay(Delay * i);
                    }
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false,
            CancellationToken cancellationToken = default)
        {
            Exception inner = null;
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    return await _storage.GetAccessControlAsync(fullPath, getUpn);
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        inner = e;
                    }
                    else
                    {
                        await Task.Delay(Delay * i);
                    }
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
        {
            Exception inner = null;
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    return await _storage.ListAsync(options, cancellationToken);
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        inner = e;
                    }
                    else
                    {
                        await Task.Delay(Delay * i);
                    }
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task<IReadOnlyCollection<Filesystem>> ListFilesystemsAsync(CancellationToken cancellationToken = default)
        {
            Exception inner = null;
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    return await _storage.ListFilesystemsAsync();
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        inner = e;
                    }
                    else
                    {
                        await Task.Delay(Delay * i);
                    }
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task SetAccessControlAsync(string fullPath, AccessControl accessControl,
            CancellationToken cancellationToken = default)
        {
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    await _storage.SetAccessControlAsync(fullPath, accessControl);
                    break;
                }
                catch (Exception e)
                {
                    if (i == Retries)
                    {
                        throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", e);
                    }

                    await Task.Delay(Delay * i);
                }
            }
        }
    }
}