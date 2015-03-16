using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileListenerPlugin
{
    public class Move : IActionModule
    {
        public string GetDescription ()
        {
            return "File Listener Get Module for detecting and downloading files";
        }

        public IEnumerable<ModuleParameterDetails> GetParameterDetails ()
        {
            yield return new ModuleParameterDetails ("searchPath", typeof (string), "search path for files.\nExample: \n* c:/[path]/[file name or wildcard expression] \n* ftp://[login:password@][server][:port]/[path]/[file name or wildcard expression] \n* s3://[login:password@][region endpoint]/[bucketname]/[path]/[file name or wildcard expression]");

            yield return new ModuleParameterDetails ("destinationPath", typeof (string), "destination path to the downloaded file");
            yield return new ModuleParameterDetails ("workFolder", typeof (string), "physical path for a temporary work area");
            yield return new ModuleParameterDetails ("backupLocation", typeof(string), "folder location to copy backup files");
            yield return new ModuleParameterDetails ("deleteSourceFile", typeof(bool), "true for delete source file, otherwise false");
            yield return new ModuleParameterDetails ("errorLocation", typeof(string), "errorLocation");

            yield return new ModuleParameterDetails ("encoding", typeof (string), "default encoding. Defaults to ISO-8859-1");
            yield return new ModuleParameterDetails ("maxFileCount", typeof (int), "maximum number of files to be processed in this pass");
            
            yield return new ModuleParameterDetails ("sshKeyFiles", typeof (string), "[SFTP] List of ssh key files for sftp");
            yield return new ModuleParameterDetails ("useReducedRedundancy", typeof (string), "[S3] reduced redundancy");
            yield return new ModuleParameterDetails ("makePublic", typeof (string), "[S3] if we should make the file public for the internet");
            yield return new ModuleParameterDetails ("partSize", typeof (string), "[S3] the size of each uploaded part");

            yield return new ModuleParameterDetails ("searchTopDirectoryOnly", typeof (bool), "Search for files on top directory or recursively");

            yield return new ModuleParameterDetails ("retryCount", typeof (int), "");
            yield return new ModuleParameterDetails ("retryWaitMs", typeof (int), "");
        }
        
        public bool Execute(ISessionContext context)
        {
            var _logger = context.GetLogger ();
            var _options = context.Options;

            IFileTransfer input = null;
            IFileTransfer output = null;
            string lastFile = null;
            int filesCount = 0;
            int maxFilesCount = Int32.MaxValue;
            FileTransferService parsedErrorLocation = new FileTransferService();

            try
            {
                var searchPath = _options.Get("searchPath", "");
                if (String.IsNullOrEmpty(searchPath))
                    throw new ArgumentNullException("searchPath");

                var destinationPath = _options.Get ("destinationPath", "");
                if (String.IsNullOrEmpty(destinationPath))
                    throw new ArgumentNullException ("destinationPath");

                var deleteSourceFile = _options.Get<bool>("deleteSourceFile", false);

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
                    _logger.Debug("No Files Found on: " + searchPath);
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
