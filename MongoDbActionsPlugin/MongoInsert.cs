using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDbActionsPlugin
{
    public class MongoInsert : IActionModule
    {
        public string GetDescription ()
        {
            return "Inser records in mongodb";
        }

        public IEnumerable<ModuleParameterDetails> GetParameterDetails ()
        {
            yield return new ModuleParameterDetails ("connectionString", typeof (string), "");

        }

        public bool Execute (ISessionContext context)
        {
            var _logger = context.GetLogger ();
            var _options = context.Options;
                        

            try
            {
                var searchPath = _options.Get ("searchPath", "");
                if (String.IsNullOrEmpty (searchPath))
                    throw new ArgumentNullException ("searchPath");

                var destinationPath = _options.Get ("destinationPath", "");
                if (String.IsNullOrEmpty (destinationPath))
                    throw new ArgumentNullException ("destinationPath");

                var deleteSourceFile = _options.Get<bool> ("deleteSourceFile", false);

                maxFilesCount = _options.Get ("maxFileCount", maxFilesCount);
                if (maxFilesCount <= 0)
                    maxFilesCount = Int32.MaxValue;

                var defaultEncoding = Encoding.GetEncoding (_options.Get ("encoding", "ISO-8859-1"));

                // prepare paths
                var parsedInput = FileTransferService.Create (searchPath, _options);
                if (!parsedInput.HasConnectionString)
                    throw new Exception ("Invalid searchPath: " + searchPath);
                var parsedOutput = FileTransferService.Create (destinationPath, _options);
                if (!parsedOutput.HasConnectionString)
                    throw new Exception ("Invalid destinationPath: " + destinationPath);

                var parsedBackupLocation = FileTransferService.Create (_options.Get ("backupLocation", ""), _options);
                parsedErrorLocation.Parse (_options.Get ("errorLocation", ""), _options);

                // open connection
                Layout layout = new Layout ();
                input = parsedInput.OpenConnection ();
                output = parsedOutput.OpenConnection ();

                // try get files
                foreach (var f in input.GetFileStreams ())
                {
                    lastFile = f.FileName;
                    _logger.Info ("File found: " + lastFile);
                    filesCount++;

                    // upload file                    
                    var fileName = parsedOutput.GetDestinationPath (f.FileName);
                    output.SendFile (f.FileStream, fileName, true);

                    context.Emit (layout.Create ()
                                      .Set ("FileName", fileName)
                                      .Set ("FileCount", filesCount)
                                      .Set ("FilePath", destinationPath)
                                      .Set ("SourcePath", searchPath)
                                      .Set ("SourceFileName", f.FileName));

                    // If backup folder exists, move file
                    if (parsedBackupLocation.HasConnectionString)
                    {
                        // TODO: implement move operation if location are the same!
                        var destName = parsedBackupLocation.GetDestinationPath (f.FileName);
                        using (var backup = parsedBackupLocation.OpenConnection ())
                            backup.SendFile (input.GetFileStream (f.FileName).FileStream, destName, true);
                    }

                    // If DeleSource is set
                    if (deleteSourceFile)
                    {
                        input.RemoveFile (f.FileName);
                    }

                    // limit
                    if (filesCount >= maxFilesCount)
                        break;
                }

                if (filesCount > 0)
                {
                    _logger.Success ("Done");
                    return true;
                }
                else
                {
                    _logger.Debug ("No Files Found on: " + searchPath);
                    return true;
                }
            }
            catch (Exception ex)
            {
                context.Error = ex.Message;
                _logger.Error (ex);
                try
                {
                    if (lastFile != null && input != null && parsedErrorLocation.HasConnectionString)
                    { // move files
                        var destName = parsedErrorLocation.GetDestinationPath (lastFile);
                        using (var backup = parsedErrorLocation.OpenConnection ())
                            backup.SendFile (input.GetFileStream (lastFile).FileStream, destName, true);
                    }
                }
                catch { }
                return false;
            }
            finally
            {
                if (output != null)
                    output.Dispose ();
                if (input != null)
                    input.Dispose ();
            }
        }
    }
}
