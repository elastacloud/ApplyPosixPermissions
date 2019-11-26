using ApplyPosixPermissions.Interfaces;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.Wrappers
{
    internal class RetryableAzureDataLakeGen2BlobStorageWrapper : IAzureDataLakeGen2BlobStorageWrapper
    {
        private IAzureDataLakeGen2BlobStorageWrapper _storage;
        private const int Retries = 3;
        private const int Delay = 1000;

        public RetryableAzureDataLakeGen2BlobStorageWrapper(IAzureDataLakeGen2BlobStorageWrapper storage)
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
                }
                catch (Exception e)
                {
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", e);
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
                }
                catch (Exception e)
                {
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", e);
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
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    inner = e;
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task<AccessControl> GetAccessControlAsync(string fullPath, bool getUpn = false)
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
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    inner = e;
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
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    inner = e;
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task<IEnumerable<string>> ListFilesystemsAsync()
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
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    inner = e;
                }
            }

            throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", inner);
        }

        public async Task SetAccessControlAsync(string fullPath, AccessControl accessControl)
        {
            for (var i = 1; i <= Retries; i++)
            {
                try
                {
                    await _storage.SetAccessControlAsync(fullPath, accessControl);
                }
                catch (Exception e)
                {
                    if (i < Retries)
                    {
                        await Task.Delay(Delay * i);
                    }

                    throw new ApplicationException($"Maximum {Retries} retries exceeded for request.", e);
                }
            }
        }
    }
}