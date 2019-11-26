using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ApplyPosixPermissions.BLL;
using ApplyPosixPermissions.Models;
using CommandLine;
using Newtonsoft.Json;

namespace ApplyPosixPermissions
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Parser.Default.ParseArguments<CommandLineOptions>(args)
                .WithParsed(o =>
                {
                    if (o.DefaultConnectionLimit != null) 
                    {
                        ServicePointManager.DefaultConnectionLimit = o.DefaultConnectionLimit.Value;
                    }

                    if (o.Expect100Continue != null)
                    {
                        ServicePointManager.Expect100Continue = o.Expect100Continue.Value;
                    }

                    var cancellationTokenSource = new CancellationTokenSource();
                    var cancellationToken = cancellationTokenSource.Token;

                    Task.Factory.StartNew(async () =>
                        {
                            var file = await File.ReadAllTextAsync(o.ConfigurationPath);
                            var directories = JsonConvert.DeserializeObject<List<DataLakeDirectory>>(file);

                            var applyPermissions = new ApplyPermissions(o.StorageAccountName, o.SharedAccessKey);
                            await applyPermissions.ProcessAsync(directories, cancellationToken);
                        }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Current)
                        .Unwrap().Wait();
                });
        }
    }
}