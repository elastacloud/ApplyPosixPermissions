using ApplyPosixPermissions.Interfaces;
using ApplyPosixPermissions.Wrappers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.Tests
{
    [TestClass]
    public class RetryableAzureDataLakeGen2BlobStorageWrapperTests
    {
        private Mock<IAzureDataLakeGen2BlobStorageWrapper> _client;
        private RetryableAzureDataLakeGen2BlobStorageWrapper _sut;

        [TestInitialize]
        public void Setup()
        {
            _client = new Mock<IAzureDataLakeGen2BlobStorageWrapper>();
            _client.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(true));
            _client.Setup(x => x.GetAccessControlAsync(It.IsAny<string>(), It.IsAny<bool>()))
                .Returns(Task.FromResult(new AccessControl("$superuser", "$superuser", "rwxr-x---", "user::rwx,group::r-x,other::---")));
            _client.Setup(x => x.ListAsync(It.IsAny<ListOptions>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult((IReadOnlyCollection<Blob>)new List<Blob>()));
            _client.Setup(x => x.ListFilesystemsAsync())
                .Returns(Task.FromResult((IEnumerable<string>)new List<string>()));

            _sut = new RetryableAzureDataLakeGen2BlobStorageWrapper(_client.Object);
        }

        [TestMethod]
        public async Task TestCreateFilesystemAsyncCallsInner()
        {
            await _sut.CreateFilesystemAsync("filesystem");
            _client.Verify(x => x.CreateFilesystemAsync("filesystem"), Times.Once);
        }

        [TestMethod]
        public async Task TestCreateFilesystemAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.CreateFilesystemAsync(It.IsAny<string>())).Throws(new ApplicationException("Test"));

            try
            {
                await _sut.CreateFilesystemAsync("filesystem");
            }
            catch
            {

            }

            _client.Verify(x => x.CreateFilesystemAsync("filesystem"), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestCreateFilesystemAsyncBubblesException()
        {
            _client.Setup(x => x.CreateFilesystemAsync(It.IsAny<string>())).Throws(new ApplicationException("Test"));
            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.CreateFilesystemAsync("filesystem"), "Test");
        }

        [TestMethod]
        public async Task TestCreateFolderAsyncCallsInner()
        {
            await _sut.CreateFolderAsync("folderPath");
            _client.Verify(x => x.CreateFolderAsync("folderPath", It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TestCreateFolderAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.CreateFolderAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new ApplicationException("Test"));

            try
            {
                await _sut.CreateFolderAsync("folderPath");
            }
            catch
            {

            }

            _client.Verify(x => x.CreateFolderAsync("folderPath", It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestCreateFolderAsyncBubblesException()
        {
            _client.Setup(x => x.CreateFolderAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new ApplicationException("Test"));

            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.CreateFolderAsync("folderPath"), "Test");
        }

        [TestMethod]
        public async Task TestExistsAsyncCallsInner()
        {
            await _sut.ExistsAsync("folderPath");
            _client.Verify(x => x.ExistsAsync("folderPath", It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TestExistsAsyncReturnsResult()
        {
            var actual = await _sut.ExistsAsync("folderPath");
            Assert.IsTrue(actual);
        }

        [TestMethod]
        public async Task TestExistsAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new ApplicationException("Test"));

            try
            {
                await _sut.ExistsAsync("folderPath");
            }
            catch
            {

            }

            _client.Verify(x => x.ExistsAsync("folderPath", It.IsAny<CancellationToken>()), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestExistsAsyncBubblesException()
        {
            _client.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new ApplicationException("Test"));

            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.ExistsAsync("folderPath"), "Test");
        }

        [TestMethod]
        public async Task TestGetAccessControlAsyncCallsInner()
        {
            await _sut.GetAccessControlAsync("folderPath", true);
            _client.Verify(x => x.GetAccessControlAsync("folderPath", true), Times.Once);
        }

        [TestMethod]
        public async Task TestGetAccessControlAsyncReturnsResult()
        {
            var actual = await _sut.GetAccessControlAsync("folderPath");
            Assert.IsNotNull(actual);
        }

        [TestMethod]
        public async Task TestGetAccessControlAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.GetAccessControlAsync(It.IsAny<string>(), It.IsAny<bool>()))
                .Throws(new ApplicationException("Test"));

            try
            {
                await _sut.GetAccessControlAsync("folderPath", true);
            }
            catch
            {

            }

            _client.Verify(x => x.GetAccessControlAsync("folderPath", true), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestGetAccessControlAsyncBubblesException()
        {
            _client.Setup(x => x.GetAccessControlAsync(It.IsAny<string>(), It.IsAny<bool>()))
                .Throws(new ApplicationException("Test"));

            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.GetAccessControlAsync("folderPath"), "Test");
        }

        [TestMethod]
        public async Task TestListAsyncCallsInner()
        {
            var expected = new ListOptions();
            await _sut.ListAsync(expected);
            _client.Verify(x => x.ListAsync(expected, It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TestListAsyncReturnsResult()
        {
            var actual = await _sut.ListAsync(new ListOptions());
            Assert.IsNotNull(actual);
        }

        [TestMethod]
        public async Task TestListAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.ListAsync(It.IsAny<ListOptions>(), It.IsAny<CancellationToken>()))
                .Throws(new ApplicationException("Test"));

            var expected = new ListOptions();

            try
            {
                await _sut.ListAsync(expected);
            }
            catch
            {

            }

            _client.Verify(x => x.ListAsync(expected, It.IsAny<CancellationToken>()), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestListAsyncBubblesException()
        {
            _client.Setup(x => x.ListAsync(It.IsAny<ListOptions>(), It.IsAny<CancellationToken>()))
                .Throws(new ApplicationException("Test"));

            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.ListAsync(new ListOptions()), "Test");
        }

        [TestMethod]
        public async Task TestListFilesystemsAsyncCallsInner()
        {
            await _sut.ListFilesystemsAsync();
            _client.Verify(x => x.ListFilesystemsAsync(), Times.Once);
        }

        [TestMethod]
        public async Task TestListFilesystemsAsyncReturnsResult()
        {
            var actual = await _sut.ListFilesystemsAsync();
            Assert.IsNotNull(actual);
        }

        [TestMethod]
        public async Task TestListFilesystemsAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.ListFilesystemsAsync())
                .Throws(new ApplicationException("Test"));

            try
            {
                await _sut.ListFilesystemsAsync();
            }
            catch
            {

            }

            _client.Verify(x => x.ListFilesystemsAsync(), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestListFilesystemsAsyncBubblesException()
        {
            _client.Setup(x => x.ListFilesystemsAsync())
                .Throws(new ApplicationException("Test"));

            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.ListFilesystemsAsync());
        }

        [TestMethod]
        public async Task TestSetAccessControlAsyncCallsInner()
        {
            var expected = new AccessControl("$superuser", "$superuser", "rwxr-x---", "user::rwx,group::r-x,other::---");
            await _sut.SetAccessControlAsync("fullPath", expected);
            _client.Verify(x => x.SetAccessControlAsync("fullPath", expected), Times.Once);
        }

        [TestMethod]
        public async Task TestSetAccessControlAsyncRetriesThreeTimes()
        {
            _client.Setup(x => x.SetAccessControlAsync(It.IsAny<string>(), It.IsAny<AccessControl>()))
                .Throws(new ApplicationException("Test"));

            var expected = new AccessControl("$superuser", "$superuser", "rwxr-x---", "user::rwx,group::r-x,other::---");

            try
            {
                await _sut.SetAccessControlAsync("fullPath", expected);
            }
            catch
            {

            }

            _client.Verify(x => x.SetAccessControlAsync("fullPath", expected), Times.Exactly(3));
        }

        [TestMethod]
        public async Task TestSetAccessControlAsyncBubblesException()
        {
            _client.Setup(x => x.SetAccessControlAsync(It.IsAny<string>(), It.IsAny<AccessControl>()))
                .Throws(new ApplicationException("Test"));

            var accessControl = new AccessControl("$superuser", "$superuser", "rwxr-x---", "user::rwx,group::r-x,other::---");
            await Assert.ThrowsExceptionAsync<ApplicationException>(() => _sut.SetAccessControlAsync("filePath", accessControl));
        }
    }
}