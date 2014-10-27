﻿using AWSRedshiftPlugin;
using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileListenerPlugin
{
    public class Get : IActionModule
    {
        string _lastError;

        public enum LoadTypes { Incremental }

        public string Name { get; private set; }

        public string Description { get; private set; }

        public Get()
        {
            Name        = "DownloadPlugin";
            Description = "Download Plugin";
        }

        public IEnumerable<PluginParameterDetails> GetParameterDetails()
        {
            yield return new PluginParameterDetails("awsAccessKey", typeof(string), "AWS Access key for S3 usage");
            yield return new PluginParameterDetails("awsSecretAccessKey", typeof(string), "AWS Secret Access key for S3 usage");
            yield return new PluginParameterDetails("searchPath", typeof(string), "search s3 path for files");
            yield return new PluginParameterDetails("physicalDownloadPath", typeof(string), "physical path to download file");
            yield return new PluginParameterDetails("backupLocation", typeof(string), "folder location to copy backup files");
            yield return new PluginParameterDetails("deleteSourceFile", typeof(bool), "true for delete source file, otherwise false");
            yield return new PluginParameterDetails("errorLocation", typeof(string), "s3 errorLocation");
        }

        public string GetLastError()
        {
            return _lastError;
        }

        public void CleanUp()
        {

        }

        Record       _options;
        ActionLogger _logger;
        AWSS3Helper  _s3;

        public void SetParameters(Record options, ActionLogger logger)
        {
            _options = options;
            _logger = logger;
        }

        public bool Execute(params IEnumerable<Record>[] dataStreams)
        {
            _lastError = null;
            List<string> files = null;
            FileSearchDetails parsedErrorLocation = null;
            try
            {
                var searchPath = _options.Get("searchPath", "");
                if (String.IsNullOrEmpty(searchPath))
                    throw new ArgumentNullException("searchPath");

                var errorLocation = _options.Get("errorLocation", "");
                if (String.IsNullOrEmpty(errorLocation))
                    throw new ArgumentNullException("errorLocation");

                var backupLocation = _options.Get("backupLocation", "");

                var physicalDownloadPath = _options.Get("physicalDownloadPath", "");
                if (String.IsNullOrEmpty(physicalDownloadPath))
                    throw new ArgumentNullException("physicalDownloadPath");

                if(!Directory.Exists(physicalDownloadPath))
                    throw new ArgumentNullException("physicalDownloadPath");

                var deleteSourceFile = _options.Get<bool>("deleteSourceFile", false);
                
                // prepare paths
                var parsedInput = FileSearchDetails.ParseSearchPath(searchPath);
                var parsedBackupLocation = FileSearchDetails.ParseSearchPath(backupLocation);
                parsedErrorLocation = FileSearchDetails.ParseSearchPath(errorLocation);

                // open s3 connection
                _s3 = new AWSS3Helper(_options.Get("awsAccessKey", ""), _options.Get("awsSecretAccessKey", ""), parsedInput.BucketName, Amazon.RegionEndpoint.USEast1, true);

                // 1. check if there is any new file                
                files = GetFilesFromS3(_s3, parsedInput).Where(f => !f.EndsWith("/")).ToList();

                if (files.Any())
                {
                    _logger.Log("Files found: " + files.Count);
                    
                    _logger.Log("Downloading files to folder");
                    // TODO: what happens if move fails?
                    foreach (var f in files)
                    {
                        // Download file
                        var downloadFrom = System.IO.Path.Combine(parsedInput.FilePath, System.IO.Path.GetFileName(f));
                        var downloadTo = System.IO.Path.Combine(physicalDownloadPath, System.IO.Path.GetFileName(f));
                        var ret = _s3.DownloadFile(downloadFrom, downloadTo);
                        // Download succeeded 
                        if (ret)
                        {
                            // If backup folder exists, move file
                            if (!String.IsNullOrEmpty(parsedBackupLocation.FilePath))
                            {
                                var BackupDestName = System.IO.Path.Combine(parsedBackupLocation.FilePath, System.IO.Path.GetFileName(f));
                                _s3.MoveFile(f, BackupDestName, false);
                            }
                            // If DeleSource is set
                            if (deleteSourceFile)
                            {
                                _s3.DeleteFile(downloadFrom, true);
                            }
                        }
                        // Download failed 
                        else
                            _logger.Error("[Error] on downloading file: " + f);
                    }
                    _logger.Success("Done");
                    return true;
                }
                else
                {
                    _logger.Success("No Files Found on: " + searchPath);
                    return true;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _logger.Error("[Error] " + _lastError);
                try
                {
                    if (files != null && _s3 != null)
                        // move files
                        foreach (var f in files)
                        {
                            var destName = System.IO.Path.Combine(parsedErrorLocation.FilePath, System.IO.Path.GetFileName(f));
                            _s3.MoveFile(f, destName, false);
                        }
                }
                catch { }
                return false;
            }
        }

        private IEnumerable<string> GetFilesFromS3(AWSS3Helper s3, FileSearchDetails parsed)
        {
            // get file from s3
            if (parsed.UseWildCardSearch)
            {
                var rgx = new System.Text.RegularExpressions.Regex(parsed.SearchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                foreach (var f in s3.GetFileList(parsed.FilePath, true, true).Where(f => !f.EndsWith("/")))
                {
                    if (rgx.IsMatch(f))
                        yield return f;
                }
            }
            else
            {
                foreach (var f in s3.GetFileList(parsed.FilePath, true, true).Where(f => !f.EndsWith("/")))
                    yield return f;
            }
        }
    }
}
