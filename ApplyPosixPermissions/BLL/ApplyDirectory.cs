using ApplyPosixPermissions.Interfaces;
using ApplyPosixPermissions.Models;
using NLog;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.BLL
{
    internal class ApplyDirectory
    {
        private const int BatchSize = 15;
        private readonly CancellationToken _cancellationToken;
        private readonly DataLakeDirectory _directory;
        private readonly ILogger _logger;
        private readonly IAzureDataLakeGen2BlobStorageWrapper _storage;
        private AccessControl _accessControl;
        private IEnumerable<AclEntry> _expectedAcls;
        private IEnumerable<AclEntry> _expectedDefaultAcls;
        private bool _isDifferent;

        public ApplyDirectory(IAzureDataLakeGen2BlobStorageWrapper storage, ILogger logger, DataLakeDirectory directory,
            CancellationToken cancellationToken)
        {
            _storage = storage;
            _logger = logger;
            _directory = directory;
            _cancellationToken = cancellationToken;
        }

        public async Task ProcessAsync()
        {
            if (_directory.Path.Split(new[] { '/' }).Length == 1)
            {
                await CreateFilesystemIfNotExistsAsync();
            }
            else
            {
                await CreateDirectoryIfNotExistsAsync();
            }

            await GetActualAclsAsync();
            GetExpectedAcls();
            GetDifferenceAcls();

            if (!_isDifferent && !_directory.Force)
            {
                return;
            }

            await SetAccessControlAsync();
            await ProcessFilesAsync();
        }

        private async Task CreateFilesystemIfNotExistsAsync()
        {
            var filesystems = await _storage.ListFilesystemsAsync();

            if (filesystems.Any(x => x == _directory.Path))
            {
                _logger.Info($"{_directory.Path} - Exists.");
            }
            else
            {
                await _storage.CreateFilesystemAsync(_directory.Path);
                _logger.Info($"{_directory.Path} - Not exists: creating filesystem.");
            }
        }

        private async Task CreateDirectoryIfNotExistsAsync()
        {
            if (await _storage.ExistsAsync(_directory.Path, _cancellationToken))
            {
                _logger.Info($"{_directory.Path} - Exists.");
            }
            else
            {
                await _storage.CreateFolderAsync(_directory.Path, cancellationToken: _cancellationToken);
                _logger.Info($"{_directory.Path} - Not exists: creating directory.");
            }
        }

        private async Task GetActualAclsAsync()
        {
            _accessControl = await _storage.GetAccessControlAsync(_directory.Path, _directory.Upn);

            _logger.Info(
                $"{_directory.Path} - Actual: {string.Join(',', _accessControl.Acl.Select(x => x.ToString()))}.");
            _logger.Info(
                $"{_directory.Path} - Actual default: {string.Join(',', _accessControl.DefaultAcl.Select(x => x.ToString()))}.");
        }

        private void GetExpectedAcls()
        {
            _expectedAcls = _directory.Acls
                .Select(x => new AclEntry(x.ObjectType, x.Identity, x.Read, x.Write, x.Execute));

            _expectedDefaultAcls = _directory.Acls
                .Select(x => new AclEntry(x.ObjectType, x.Identity, x.DefaultRead, x.DefaultWrite, x.DefaultExecute));

            _logger.Info(
                $"{_directory.Path} - Expected: {string.Join(',', _expectedAcls.Select(x => x.ToString()))}.");
            _logger.Info(
                $"{_directory.Path} - Expected default: {string.Join(',', _expectedDefaultAcls.Select(x => x.ToString()))}.");
        }

        private void GetDifferenceAcls()
        {
            var expected = _expectedAcls.Select(x => new
            {
                Default = false,
                Acl = x.ToString(),
                Type = "expected"
            }).Concat(_expectedDefaultAcls.Select(x => new
            {
                Default = true,
                Acl = x.ToString(),
                Type = "expected"
            }));

            var actual = _accessControl.Acl.Select(x => new
            {
                Default = false,
                Acl = x.ToString(),
                Type = "actual"
            }).Concat(_accessControl.DefaultAcl.Select(x => new
            {
                Default = true,
                Acl = x.ToString(),
                Type = "actual"
            }));

            var additionalExpected = from e in expected
                                     join a in actual
                                         on new { e.Default, e.Acl }
                                         equals new { a.Default, a.Acl } into lja
                                     from a in lja.DefaultIfEmpty()
                                     where a == null
                                     select e;

            var additionalActual = from a in actual
                                   join e in expected
                                       on new { a.Default, a.Acl }
                                       equals new { e.Default, e.Acl } into lje
                                   from e in lje.DefaultIfEmpty()
                                   where e == null
                                   select a;

            var differences = additionalExpected.Union(additionalActual);

            _logger.Info(
                $"{_directory.Path} - Differences: {string.Join(',', differences.Where(x => !x.Default).Select(x => $"{x.Type}: {x.Acl}"))}");
            _logger.Info(
                $"{_directory.Path} - Differences default: {string.Join(',', differences.Where(x => x.Default).Select(x => $"{x.Type}: {x.Acl}"))}");

            _isDifferent = differences.Any();
        }

        private async Task SetAccessControlAsync()
        {
            _accessControl.Acl.Clear();
            _accessControl.DefaultAcl.Clear();

            foreach (var acl in _expectedAcls)
            {
                _accessControl.Acl.Add(acl);
            }

            foreach (var acl in _expectedDefaultAcls)
            {
                _accessControl.DefaultAcl.Add(acl);
            }

            await _storage.SetAccessControlAsync(_directory.Path, _accessControl);
        }

        private async Task ProcessFilesAsync()
        {
            var files = await _storage.ListAsync(new ListOptions
            {
                FolderPath = _directory.Path,
                MaxResults = int.MaxValue,
                Recurse = _directory.Recurse
            }, _cancellationToken);

            var batches = files
                .Select((x, y) => new { Item = x, Index = y })
                .GroupBy(x => x.Index / BatchSize)
                .Select(x => x.Select(y =>
                {
                    _logger.Info($"{_directory.Path} - Applying to file: {y.Item.FullPath}.");
                    return new ApplyBlob(_storage, y.Item, _expectedAcls, _expectedDefaultAcls, _directory.Upn).ProcessAsync();
                }));

            foreach (var batch in batches)
            {
                await Task.WhenAll(batch);
            }
        }
    }
}