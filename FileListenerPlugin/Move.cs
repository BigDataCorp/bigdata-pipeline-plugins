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
            yield return new ModuleParameterDetails ("inputPath", typeof (string), "search path for files.\nExample: \n* c:/[path]/[file name or wildcard expression] \n* ftp://[login:password@][server][:port]/[path]/[file name or wildcard expression] \n* s3://[login:password@][region endpoint]/[bucketname]/[path]/[file name or wildcard expression]");

            yield return new ModuleParameterDetails ("outputPath", typeof (string), "destination path to the downloaded file", true);
            yield return new ModuleParameterDetails ("backupLocation", typeof(string), "folder location to copy backup files");
            yield return new ModuleParameterDetails ("deleteSourceFile", typeof(bool), "true for delete source file, otherwise false");
            yield return new ModuleParameterDetails ("errorLocation", typeof(string), "errorLocation");

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
            var fileTransferService = context.GetContainer ().GetInstanceOf<IFileService> ();
            var deleteSourceFile = _options.Get<bool> ("deleteSourceFile", false);

            try
            {
                var searchPath = _options.Get ("inputPath", _options.Get ("searchPath", ""));
                if (String.IsNullOrEmpty(searchPath))
                    throw new ArgumentNullException ("inputPath");

                var destinationPath = _options.Get ("outputPath", "");
                if (String.IsNullOrEmpty(destinationPath))
                    throw new ArgumentNullException ("outputPath");

                maxFilesCount = _options.Get ("maxFileCount", maxFilesCount);
                if (maxFilesCount <= 0)
                    maxFilesCount = Int32.MaxValue;

                // prepare paths
                input = fileTransferService.Open (searchPath, _options);
                if (!input.IsOpened ())
                    throw new Exception (String.Format ("Invalid inputPath, {0}: {1}", input.LastError ?? "", searchPath));
                output = fileTransferService.Open (destinationPath, _options);
                if (!output.IsOpened ())
                    throw new Exception (String.Format ("Invalid destinationPath, {0}: {1}", output.LastError ?? "", destinationPath));

                // open connection
                Layout layout = new Layout ();

                // try get files
                foreach (var f in input.GetFileStreams ())
                {
                    lastFile = f.FileName;
                    _logger.Info ("File found: " + lastFile);
                    filesCount++;

                    // upload file                    
                    var fileName = output.Details.GetDestinationPath (f.FileName);
                    if (!output.SendFile (f.FileStream, fileName, true))
                    {
                        _logger.Error (output.LastError);
                        // move to error folder
                        if (!String.IsNullOrEmpty (_options.Get ("errorLocation", "")))
                        {
                            // move files                        
                            using (var parsedErrorLocation = fileTransferService.Open (_options.Get ("errorLocation", ""), _options))
                            {
                                var destName = parsedErrorLocation.Details.GetDestinationPath (lastFile);
                                if (parsedErrorLocation.SendFile (input.GetFileStream (lastFile).FileStream, destName, true) && deleteSourceFile)
                                {
                                    // If DeleSource is set
                                    input.RemoveFile (lastFile);
                                    _logger.Info ("File deleted: " + lastFile);
                                }
                            }
                        }
                        // continue to next file
                        continue;
                    }

                    context.Emit (layout.Create ()
                                      .Set ("fileName", fileName)
                                      .Set ("fileNumber", filesCount)
                                      .Set ("filePath", destinationPath)
                                      .Set ("sourcePath", searchPath)
                                      .Set ("sourceFileName", f.FileName));
                    
                    // If backup folder exists, move file
                    if (!String.IsNullOrWhiteSpace (_options.Get ("backupLocation", "")))
                    {
                        // TODO: implement move operation if location are the same!
                        using (var backupLocation = fileTransferService.Open (_options.Get ("backupLocation", ""), _options))
                        {
                            var destName = backupLocation.Details.GetDestinationPath (f.FileName);
                            backupLocation.SendFile (input.GetFileStream (f.FileName).FileStream, destName, true);
                            _logger.Info ("Backup file created: " + destName);
                        }
                    }

                    // If DeleSource is set
                    if (deleteSourceFile)
                    {
                        input.RemoveFile (f.FileName);
                        _logger.Info ("File deleted: " + f.FileName);
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
                    if (lastFile != null && input != null && !String.IsNullOrEmpty (_options.Get ("errorLocation", "")))
                    {
                        // move files                        
                        using (var parsedErrorLocation = fileTransferService.Open (_options.Get ("errorLocation", ""), _options))
                        {
                            var destName = parsedErrorLocation.Details.GetDestinationPath (lastFile);
                            if (parsedErrorLocation.SendFile (input.GetFileStream (lastFile).FileStream, destName, true) && deleteSourceFile)
                            {
                                // If DeleSource is set
                                input.RemoveFile (lastFile);
                                _logger.Info ("File deleted: " + lastFile);
                            }
                        }
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
