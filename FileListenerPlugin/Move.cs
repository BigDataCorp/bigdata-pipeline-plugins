﻿using BigDataPipeline.Interfaces;
using FileListenerPlugin.SimpleHelpers;
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
            yield return new ModuleParameterDetails ("maxConcurrency", typeof (int), "maximum number of concurrent trasnfers. Default to 2.");
            
            yield return new ModuleParameterDetails ("sshKeyFiles", typeof (string), "[SFTP] List of ssh key files for sftp");
            yield return new ModuleParameterDetails ("useReducedRedundancy", typeof (string), "[S3] reduced redundancy");
            yield return new ModuleParameterDetails ("makePublic", typeof (string), "[S3] if we should make the file public for the internet");
            yield return new ModuleParameterDetails ("partSize", typeof (string), "[S3] the size of each uploaded part");

            yield return new ModuleParameterDetails ("searchTopDirectoryOnly", typeof (bool), "Search for files on top directory or recursively");

            yield return new ModuleParameterDetails ("retryCount", typeof (int), "");
            yield return new ModuleParameterDetails ("retryWaitMs", typeof (int), "");
        }

        ISessionContext _context;
        IActionLogger _logger;
        FlexibleObject _options;
        int _filesCount = 0;
        int _maxFilesCount = Int32.MaxValue;
        bool _deleteSourceFile = false;
        IFileService _fileTransferService;
        Layout _layout = new Layout ();

        public bool Execute(ISessionContext context)
        {
            _context = context;
            _logger = context.GetLogger ();
            _options = context.Options;

            IFileTransfer input = null;
            _fileTransferService = context.GetContainer ().GetInstanceOf<IFileService> ();
            _deleteSourceFile = _options.Get<bool> ("deleteSourceFile", false);

            try
            {
                var searchPath = _options.Get ("inputPath", _options.Get ("searchPath", ""));
                if (String.IsNullOrEmpty (searchPath))
                    throw new ArgumentNullException ("inputPath");

                if (String.IsNullOrEmpty (_options.Get ("outputPath", "")))
                    throw new ArgumentNullException ("outputPath");

                _maxFilesCount = _options.Get ("maxFileCount", _maxFilesCount);
                if (_maxFilesCount <= 0)
                    _maxFilesCount = Int32.MaxValue;

                // prepare paths
                input = _fileTransferService.Open (searchPath, _options);
                if (!input.IsOpened ())
                    throw new Exception (String.Format ("Invalid inputPath, {0}: {1}", input.LastError ?? "", searchPath));

                // try move files
                ParallelTasks<StreamTransferInfo>.Process (input.GetFileStreams ().Take (_maxFilesCount), 1, _options.Get ("maxConcurrency", 2), f => MoveFileInternal (f, searchPath));                

                if (_filesCount > 0)
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
                return false;
            }
            finally
            {
                if (input != null)
                    input.Dispose ();                
            }
        }
 
        private bool MoveFileInternal (StreamTransferInfo f, string searchPath)
        {
            _logger.Info ("File found: " + f.FileName);
            _filesCount++;
            IFileTransfer output = null;

            try
            {
                var destinationPath = _options.Get ("outputPath", "");
                // open destination
                output = _fileTransferService.Open (destinationPath, _options);
                if (!output.IsOpened ())
                    throw new Exception (String.Format ("Invalid destinationPath, {0}: {1}", output.LastError ?? "", destinationPath));

                // upload file                    
                var fileName = output.Details.GetDestinationPath (f.FileName);
                if (!output.SendFile (f.FileStream, fileName, true))
                {
                    _logger.Error (output.LastError);
                            
                    // move to error folder
                    MoveToErrorLocation (f.FileName, searchPath);
                    // continue to next file
                    return true;
                }

                // emit file info
                lock (_context)
                {
                    _context.Emit (_layout.Create ()
                                       .Set ("fileName", fileName)
                                       .Set ("fileNumber", _filesCount)
                                       .Set ("filePath", destinationPath)
                                       .Set ("sourcePath", searchPath)
                                       .Set ("sourceFileName", f.FileName));
                }
                    
                // If backup folder exists, move file
                MoveToBackupLocation (f.FileName, searchPath);

                // If DeleSource is set
                if (_deleteSourceFile)
                {
                    TryDeleteFile (f.FileName, searchPath);
                }
            }
            catch (Exception ex)
            {
                _context.Error = ex.Message;
                _logger.Error (ex);
                MoveToErrorLocation (f.FileName, searchPath);
                return false;
            }
            finally
            {
                if (output != null)
                    output.Dispose ();
            }
            return true;
        }

        private bool TryDeleteFile (string filename, string inputPathUrl)
        {
            try
            {
                using (var input = _fileTransferService.Open (inputPathUrl, _options))
                    input.RemoveFile (filename);
                _logger.Info ("File deleted: " + filename);
            }
            catch (Exception ex)
            {
                _logger.Error (ex);
                return false;
            }
            return true;
        }

        private bool MoveToBackupLocation (string filename, string inputPathUrl)
        {
            try
            {
                // If backup folder exists, move file
                if (!String.IsNullOrWhiteSpace (_options.Get ("backupLocation", "")))
                {
                    // TODO: implement move operation if location are the same!
                    using (var backupLocation = _fileTransferService.Open (_options.Get ("backupLocation", ""), _options))
                    {
                        if (backupLocation.IsOpened ())
                        {
                            using (var input = _fileTransferService.Open (inputPathUrl, _options))
                            {
                                var destName = backupLocation.Details.GetDestinationPath (filename);
                                backupLocation.SendFile (input.GetFileStream (filename).FileStream, destName, true);
                                _logger.Info ("Backup file created: " + destName);
                            }
                        }
                        else
                        {
                            _logger.Error ("Fail to open backup location: " + backupLocation.LastError);
                            return false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error (ex);
                return false;
            }
            return true;
        }
 
        private bool MoveToErrorLocation (string filename, string inputPathUrl)
        {
            try
            {
                // move to error folder
                if (!String.IsNullOrEmpty (_options.Get ("errorLocation", "")))
                { 
                    // move files                        
                    using (var parsedErrorLocation = _fileTransferService.Open (_options.Get ("errorLocation", ""), _options))
                    {
                        if (parsedErrorLocation.IsOpened ())
                        {
                            using (var input = _fileTransferService.Open (inputPathUrl, _options))
                            {
                                var destName = parsedErrorLocation.Details.GetDestinationPath (filename);
                                if (parsedErrorLocation.SendFile (input.GetFileStream (filename).FileStream, destName, true) && _deleteSourceFile)
                                {
                                    // If DeleSource is set
                                    input.RemoveFile (filename);
                                    _logger.Info (String.Format ("File moved to error path: {0} to {1}", filename, destName));
                                }
                            }
                        }
                        else
                        {
                            _logger.Error ("Fail to open error location: " + parsedErrorLocation.LastError);
                            return false;
                        }
                    }
                }
            }
            catch (Exception ex)
            { 
                _logger.Error (ex);
                return false;
            }
            return true;
        }
        
    }
}
