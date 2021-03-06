using ApplyPosixPermissions.BLL;
using ApplyPosixPermissions.Interfaces;
using ApplyPosixPermissions.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using NLog;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.Tests
{
    [TestClass]
    public class ApplyPermissionsTests
    {
        private Mock<IAzureDataLakeStorageWrapper> _azureDataLakeStorage;
        private List<DataLakeDirectory> _directories;
        private Mock<ILogger> _logger;
        private ApplyPermissions _sut;

        [TestInitialize]
        public void Setup()
        {
            _directories = new List<DataLakeDirectory>
            {
                new DataLakeDirectory
                {
                    Path = "raw/directory",
                    Upn = true,
                    Recurse = true,
                    Acls = new List<Acl>
                    {
                        new Acl
                        {
                            Read = true,
                            Write = true,
                            Execute = true,
                            DefaultRead = true,
                            DefaultWrite = true,
                            DefaultExecute = true,
                            Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                            ObjectType = ObjectType.User
                        },
                        new Acl
                        {
                            Read = true,
                            Write = true,
                            Execute = true,
                            DefaultRead = true,
                            DefaultWrite = true,
                            DefaultExecute = true,
                            Identity = "c20047f4-79e8-4446-b441-b1ea03a8e17d",
                            ObjectType = ObjectType.User
                        }
                    }
                },
                new DataLakeDirectory
                {
                    Path = "trusted/directory",
                    Upn = true,
                    Recurse = true,
                    Acls = new List<Acl>
                    {
                        new Acl
                        {
                            Read = true,
                            Write = true,
                            Execute = true,
                            DefaultRead = true,
                            DefaultWrite = true,
                            DefaultExecute = true,
                            Identity = "c20047f4-79e8-4446-b441-b1ea03a8e17d",
                            ObjectType = ObjectType.User
                        }
                    }
                }
            };

            var rawAccessControl = new AccessControl("$superuser", "$superuser", "rwxrwx---+",
                "user::rwx,user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:rwx,group::r-x,mask::rwx,other::---,default:user::rwx,default:user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,default:user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:rwx,default:group::r-x,default:mask::rwx,default:other::---");

            var trustedAccessControl = new AccessControl("$superuser", "$superuser", "rwxrwx---+",
                "user::rwx,user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,group::r-x,mask::rwx,other::---,default:user::rwx,default:user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,default:group::r-x,default:mask::rwx,default:other::---");

            var file1AccessControl = new AccessControl("c20047f4-79e8-4446-b441-b1ea03a8e17d", "$superuser",
                "rw-r-----+",
                "user::rw-,user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:rwx,group::r-x,mask::r--,other::---");

            var file2AccessControl = new AccessControl("c20047f4-79e8-4446-b441-b1ea03a8e17d", "$superuser",
                "rw-r-----+",
                "user::rw-,user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:rwx,group::r-x,mask::r--,other::---");

            var subDirectoryAccessControl = new AccessControl("$superuser", "$superuser", "rwxrwx---+",
                "user::rwx,user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:rwx,group::r-x,mask::rwx,other::---,default:user::rwx,default:user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx,default:user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:rwx,default:group::r-x,default:mask::rwx,default:other::---");

            var newAccessControl = new AccessControl("$superuser", "$superuser", "rwxr-x---", "user::rwx,group::r-x,other::---");

            _azureDataLakeStorage = new Mock<IAzureDataLakeStorageWrapper>();

            _azureDataLakeStorage.Setup(x => x.ExistsAsync("raw/directory", It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(true));

            _azureDataLakeStorage.Setup(x => x.ExistsAsync("trusted/directory", It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(true));

            _azureDataLakeStorage.Setup(x => x.ListFilesystemsAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(
                    (IReadOnlyCollection<Filesystem>)new List<Filesystem> { new Filesystem { Name = "new" } })
                );

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("raw/directory", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(rawAccessControl));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("trusted/directory", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(trustedAccessControl));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("trusted/directory/file1.parquet", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(file1AccessControl));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("trusted/directory/file2.parquet", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(file2AccessControl));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("trusted/directory/subdirectory", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(subDirectoryAccessControl));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("new", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(newAccessControl));

            _azureDataLakeStorage
                .Setup(x => x.SetAccessControlAsync(It.IsAny<string>(), It.IsAny<AccessControl>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _azureDataLakeStorage
                .Setup(x => x.ListAsync(It.Is<ListOptions>(y => y.FolderPath == "trusted/directory"),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult((IReadOnlyCollection<Blob>)new List<Blob>
                {
                    new Blob("trusted/directory/file1.parquet"),
                    new Blob("trusted/directory/file2.parquet"),
                    new Blob("trusted/directory/subdirectory", BlobItemKind.Folder)
                }));

            _azureDataLakeStorage
                .Setup(x => x.ListAsync(It.Is<ListOptions>(y => y.FolderPath != "trusted/directory"),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult((IReadOnlyCollection<Blob>)new List<Blob>()));

            _logger = new Mock<ILogger>();
            _sut = new ApplyPermissions(_azureDataLakeStorage.Object, _logger.Object);
        }

        [TestMethod]
        public async Task TestCallsDirectoryExists()
        {
            await _sut.ProcessAsync(_directories);
            _directories.ForEach(x =>
                _azureDataLakeStorage.Verify(y => y.ExistsAsync(x.Path, It.IsAny<CancellationToken>()))
            );
        }

        [TestMethod]
        public async Task TestDoesNotCreateDirectoryIfExists()
        {
            await _sut.ProcessAsync(_directories);
            _directories.ForEach(x =>
                _azureDataLakeStorage.Verify(
                    y => y.CreateFolderAsync(x.Path, null, It.IsAny<CancellationToken>()), Times.Never)
            );
        }

        [TestMethod]
        public async Task TestCreatesDirectoryIfNotExists()
        {
            _azureDataLakeStorage.Setup(x => x.ExistsAsync("trusted/directory", It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(false));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("trusted/directory", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new AccessControl("$superuser", "$superuser", "rwxr-x---",
                    "user::rwx,group::r-x,other::---")));

            await _sut.ProcessAsync(_directories);

            _azureDataLakeStorage.Verify(
                x => x.CreateFolderAsync("raw/directory", null, It.IsAny<CancellationToken>()), Times.Never);

            _azureDataLakeStorage.Verify(
                x => x.CreateFolderAsync("trusted/directory", null, It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TestCallsListFilesystems()
        {
            await _sut.ProcessAsync(new List<DataLakeDirectory>
            {
                new DataLakeDirectory
                {
                    Path = "new",
                    Upn = true,
                    Recurse = true,
                    Acls = new List<Acl>()
                }
            });

            _azureDataLakeStorage.Verify(y => y.ListFilesystemsAsync(It.IsAny<CancellationToken>()));
        }

        [TestMethod]
        public async Task TestDoesNotCreateFilesystemIfExists()
        {
            await _sut.ProcessAsync(new List<DataLakeDirectory>
            {
                new DataLakeDirectory
                {
                    Path = "new",
                    Upn = true,
                    Recurse = true,
                    Acls = new List<Acl>()
                }
            });

            _azureDataLakeStorage.Verify(y => y.CreateFilesystemAsync("new"), Times.Never);
        }

        [TestMethod]
        public async Task TestCreatesFilesystemIfNotExists()
        {
            _azureDataLakeStorage.Setup(x => x.ListFilesystemsAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult((IReadOnlyCollection<Filesystem>)new List<Filesystem>().AsEnumerable()));

            await _sut.ProcessAsync(new List<DataLakeDirectory>
            {
                new DataLakeDirectory
                {
                    Path = "new",
                    Upn = true,
                    Recurse = true,
                    Acls = new List<Acl>()
                }
            });

            _azureDataLakeStorage.Verify(y => y.CreateFilesystemAsync("new"), Times.Once);
        }

        [TestMethod]
        public async Task TestDoesNotUpdateDirectoryIfNoDifferencesDetected()
        {
            await _sut.ProcessAsync(_directories);
            _azureDataLakeStorage.Verify(
                x => x.SetAccessControlAsync(It.IsAny<string>(), It.IsAny<AccessControl>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [TestMethod]
        public async Task TestDoesNotUpdateFilesIfNoDifferencesDetected()
        {
            await _sut.ProcessAsync(_directories);
            _azureDataLakeStorage.Verify(
                x => x.ListAsync(It.IsAny<ListOptions>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [TestMethod]
        public async Task TestUpdatesDirectoryIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _directories.First(x => x.Path == "trusted/directory").Acls.ForEach(x =>
            {
                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanRead == x.Read),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanWrite == x.Write),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanExecute == x.Execute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory",
                        It.Is<AccessControl>(z =>
                            z.DefaultAcl.First(a => a.Identity == x.Identity).CanRead == x.DefaultRead),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory",
                        It.Is<AccessControl>(z =>
                            z.DefaultAcl.First(a => a.Identity == x.Identity).CanWrite == x.DefaultWrite),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory",
                        It.Is<AccessControl>(z =>
                            z.DefaultAcl.First(a => a.Identity == x.Identity).CanExecute == x.DefaultExecute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            });
        }

        [TestMethod]
        public async Task TestListsFilesIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _azureDataLakeStorage.Verify(x =>
                x.ListAsync(It.Is<ListOptions>(y => y.FolderPath == "trusted/directory"),
                    It.IsAny<CancellationToken>()), Times.Once);

            _azureDataLakeStorage.Verify(x =>
                x.ListAsync(It.Is<ListOptions>(y => y.MaxResults == int.MaxValue),
                    It.IsAny<CancellationToken>()), Times.Once);

            _azureDataLakeStorage.Verify(x =>
                x.ListAsync(It.Is<ListOptions>(y => y.Recurse),
                    It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TestGetsAccessControlForFilesIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _azureDataLakeStorage.Verify(x => x.GetAccessControlAsync("trusted/directory/file1.parquet", true, It.IsAny<CancellationToken>()),
                Times.Once);
            _azureDataLakeStorage.Verify(x => x.GetAccessControlAsync("trusted/directory/file2.parquet", true, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task TestSetsAccessControlForFilesIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _directories.First(x => x.Path == "trusted/directory").Acls.ForEach(x =>
            {
                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanRead == x.Read),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanWrite == x.Write),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanExecute == x.Execute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanRead == x.Read),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanWrite == x.Write),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanExecute == x.Execute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            });
        }

        [TestMethod]
        public async Task TestDoesNotSetDefaultAccessControlForFilesIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _directories.First(x => x.Path == "trusted/directory").Acls.ForEach(x =>
            {
                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                    It.Is<AccessControl>(z => !z.DefaultAcl.Any()), It.IsAny<CancellationToken>()), Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                    It.Is<AccessControl>(z => !z.DefaultAcl.Any()), It.IsAny<CancellationToken>()), Times.Once);
            });
        }

        [TestMethod]
        public async Task TestSetsAccessControlForSubDirectoriesIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _directories.First(x => x.Path == "trusted/directory").Acls.ForEach(x =>
            {
                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/subdirectory",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanRead == x.Read),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/subdirectory",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanWrite == x.Write),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/subdirectory",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanExecute == x.Execute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            });
        }

        [TestMethod]
        public async Task TestSetsDefaultAccessControlForSubDirectoriesIfDifferenceDetected()
        {
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _directories.First(x => x.Path == "trusted/directory").Acls.ForEach(x =>
            {
                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/subdirectory",
                        It.Is<AccessControl>(z =>
                            z.DefaultAcl.First(a => a.Identity == x.Identity).CanRead == x.DefaultRead),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/subdirectory",
                        It.Is<AccessControl>(z =>
                            z.DefaultAcl.First(a => a.Identity == x.Identity).CanWrite == x.DefaultWrite),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/subdirectory",
                        It.Is<AccessControl>(z =>
                            z.DefaultAcl.First(a => a.Identity == x.Identity).CanExecute == x.DefaultExecute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            });
        }

        [TestMethod]
        public async Task TestSetsAccessControlForFilesIfForce()
        {
            _directories.First(x => x.Path == "trusted/directory").Force = true;

            await _sut.ProcessAsync(_directories);

            _directories.First(x => x.Path == "trusted/directory").Acls.ForEach(x =>
            {
                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanRead == x.Read),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanWrite == x.Write),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file1.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanExecute == x.Execute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanRead == x.Read),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanWrite == x.Write),
                        It.IsAny<CancellationToken>()),
                    Times.Once);

                _azureDataLakeStorage.Verify(y => y.SetAccessControlAsync("trusted/directory/file2.parquet",
                        It.Is<AccessControl>(z =>
                            z.Acl.First(a => a.Identity == x.Identity).CanExecute == x.Execute),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            });
        }

        [TestMethod]
        public async Task TestLogsDirectoryExists()
        {
            await _sut.ProcessAsync(_directories);

            _directories.ForEach(x => _logger.Verify(y => y.Info($"{x.Path} - Exists.")));
        }

        [TestMethod]
        public async Task TestLogCreatesDirectoryIfNotExists()
        {
            _azureDataLakeStorage.Setup(x => x.ExistsAsync("trusted/directory", It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(false));

            _azureDataLakeStorage.Setup(x => x.GetAccessControlAsync("trusted/directory", true, It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new AccessControl("$superuser", "$superuser", "rwxr-x---",
                    "user::rwx,group::r-x,other::---")));

            await _sut.ProcessAsync(_directories);

            _logger.Verify(x => x.Info("trusted/directory - Not exists: creating directory."));
        }

        [TestMethod]
        public async Task TestLogsActualAcls()
        {
            _directories.Remove(_directories.First(x => x.Path == "raw/directory"));
            await _sut.ProcessAsync(_directories);

            _logger.Verify(x => x.Info("trusted/directory - Actual: user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx."));
            _logger.Verify(x =>
                x.Info("trusted/directory - Actual default: user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx."));
        }

        [TestMethod]
        public async Task TestLogsExpectedAcls()
        {
            _directories.Remove(_directories.First(x => x.Path == "raw/directory"));
            await _sut.ProcessAsync(_directories);

            _logger.Verify(x => x.Info("trusted/directory - Expected: user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx."));
            _logger.Verify(x =>
                x.Info("trusted/directory - Expected default: user:c20047f4-79e8-4446-b441-b1ea03a8e17d:rwx."));
        }

        [TestMethod]
        public async Task TestLogsDifferenceAcls()
        {
            _directories.Remove(_directories.First(x => x.Path == "raw/directory"));
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _logger.Verify(x =>
                x.Info("trusted/directory - Differences: expected: user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:r-x"));
            _logger.Verify(x =>
                x.Info(
                    "trusted/directory - Differences default: expected: user:ea6be951-d694-4b49-bd5c-fef06e7b9a59:r-x"));
        }

        [TestMethod]
        public async Task TestLogsFileAcls()
        {
            _directories.Remove(_directories.First(x => x.Path == "raw/directory"));
            _directories.First(x => x.Path == "trusted/directory").Acls.Add(new Acl
            {
                Read = true,
                Write = false,
                Execute = true,
                DefaultRead = true,
                DefaultWrite = false,
                DefaultExecute = true,
                Identity = "ea6be951-d694-4b49-bd5c-fef06e7b9a59",
                ObjectType = ObjectType.User
            });

            await _sut.ProcessAsync(_directories);

            _logger.Verify(x => x.Info("trusted/directory - Applying to file: trusted/directory/file1.parquet."));
            _logger.Verify(x => x.Info("trusted/directory - Applying to file: trusted/directory/file2.parquet."));
        }

        [TestMethod]
        public async Task TestLogsException()
        {
            var expected = new Exception("Test exception.");

            _azureDataLakeStorage.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(expected);

            try
            {
                await _sut.ProcessAsync(_directories);
            }
            catch
            {
            }

            _logger.Verify(x => x.Error(expected));
        }

        [TestMethod]
        public async Task TestRethrowsException()
        {
            var expected = new Exception("Test exception.");

            _azureDataLakeStorage.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(expected);

            try
            {
                await _sut.ProcessAsync(_directories);
            }
            catch (Exception actual)
            {
                Assert.AreEqual(expected, actual);
            }
        }
    }
}