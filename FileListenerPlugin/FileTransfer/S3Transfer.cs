using System;
using System.Linq;
using System.IO;
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace FileListenerPlugin
{
    public class S3Transfer : IFileTransfer 
    {
        AWSS3Helper client;        

        public FileTransferDetails Details { get; private set; }

        public FileTranferStatus Status { get; private set; }

        public string LastError { get; private set; }

        private void _setStatus (Exception message)
        {
            string msg = null;
            if (message != null && message.Message != null)
            {
                msg = message.Message;
                if (message.InnerException != null && message.InnerException.Message != null)
                    msg += "; " + message.InnerException.Message;
            }
            
            _setStatus (msg != null, msg);
        }

        private void _setStatus (bool status, string message = null)
        {
            Status = status ? FileTranferStatus.Success : FileTranferStatus.Error;
            LastError = message;
        }
                
        public bool Open (FileTransferDetails details)
        { 
            Details = details;
            if (Details.RetryCount <= 0)
                Details.RetryCount = 1;
            return Open ();
        }
        
        private bool Open ()
        {
            if (IsOpened ())
                return true;

            var bucketName = Details.Get ("bucketName", "");
            // sanity checks
            if (String.IsNullOrEmpty (bucketName))
            {
                _setStatus (false, "Empty S3 Bucket Name");
                return false;
            }

            var s3Region = Amazon.RegionEndpoint.GetBySystemName (Details.Get ("awsRegion", "us-east-1"));
            if (s3Region.DisplayName == "Unknown")
            {
                _setStatus (false, "Invalid S3 Region Endpoint");
                return false;
            }

            for (var i = 0; i < Details.RetryCount; i++)
            {             
                try
                {
                    client = new AWSS3Helper (Details.Login, Details.Password, bucketName, s3Region, true);

                    _setStatus (true);
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    Close ();
                }
                if (client != null)
                    break;
                System.Threading.Thread.Sleep (Details.RetryWaitMs);
            }
            return IsOpened ();
        }        

        private string PreparePath (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                folder = "/";
            folder = folder.Replace ('\\', '/');
            if (!folder.StartsWith ("/"))
                folder = "/" + folder;
            return folder;
        }

        public bool IsOpened ()
        {
            return client != null;
        }

        public void Dispose ()
        {
            Close ();
        }

        private void Close ()
        {
            if (client != null)
            {
                try
                {
                    using (var ptr = client)
                    {
                        client = null;                        
                    }
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                }
            }
            client = null;
        }

        private IEnumerable<string> _listFiles (string folder, System.Text.RegularExpressions.Regex pattern, bool recursive)
        {
            _setStatus (true);

            if (!Open ())
                yield break;

            folder = PreparePath (folder);

            foreach (var f in client.GetFileList (folder, recursive, false))
                if (!f.EndsWith ("/") && (pattern == null || pattern.IsMatch (System.IO.Path.GetFileName (f))))
                    yield return f;
        }
        
        public IEnumerable<string> ListFiles ()
        {
            return ListFiles (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
        }

        public IEnumerable<string> ListFiles (string folder, bool recursive)
        { 
            return _listFiles (folder, null, recursive);
        }

        public IEnumerable<string> ListFiles (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferDetails.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
            return _listFiles (folder, pattern, recursive);
        }

        public IEnumerable<StreamTransfer> GetFileStreams (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferDetails.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

            _setStatus (true);
            // download files
            foreach (var f in _listFiles (folder, pattern, recursive))
            {
                yield return new StreamTransfer
                {
                    FileName = f,
                    FileStream = client.ReadFile (f, false)
                };
            }
        }

        public IEnumerable<StreamTransfer> GetFileStreams ()
        {
            return GetFileStreams (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
        }

        public IEnumerable<string> GetFiles (string folder, string fileMask, bool recursive, string outputDirectory, bool deleteOnSuccess)
        {
            outputDirectory = outputDirectory.Replace ('\\', '/');
            if (!outputDirectory.EndsWith ("/"))
                outputDirectory += "/";
            FileTransferDetails.CreateDirectory (outputDirectory);

            // download files
            foreach (var f in GetFileStreams (folder, fileMask, recursive))
            {
                string newFile = null;                
                
                // download file
                try
                { 
                    newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
                    FileTransferDetails.DeleteFile (newFile);

                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultWriteBufferSize))
                    {
                        f.FileStream.CopyTo (file, FileTransferDetails.DefaultWriteBufferSize >> 2);
                    }

                    f.FileStream.Close ();
                    f.FileStream = null;

                    // check if we must remove file
                    if (deleteOnSuccess && System.IO.File.Exists (newFile))
                    {   
                        client.DeleteFile (f.FileName);
                    }

                    _setStatus (true);
                    break;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    Close ();
                    FileTransferDetails.DeleteFile (newFile);
                    newFile = null;
                    // delay in case of network error
                    System.Threading.Thread.Sleep (Details.RetryWaitMs);
                    // try to reopen
                    Open ();
                }
                finally
                {
                    if (f.FileStream != null)
                        f.FileStream.Close ();
                }

                // check if file was downloaded
                if (newFile != null && System.IO.File.Exists (newFile))
                {
                    yield return newFile;
                }
            }
        }

        public IEnumerable<string> GetFiles (string outputDirectory, bool deleteOnSuccess)
        {
            return GetFiles (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly, outputDirectory, deleteOnSuccess);
        }

        public bool RemoveFiles (IEnumerable<string> files)
        {
            var enumerator = files.GetEnumerator ();            
            string file = null;
            for (int i = 0; i < Details.RetryCount; i++)
            {
                // try to open
                if (!Open ())
                    continue;
                     
                try
                {
                    if (file == null)
                    {
                        enumerator.MoveNext ();                                                
                    }

                    do
                    {
                        file = PreparePath (enumerator.Current);
                        client.DeleteFile (file);               
                    }
                    while (enumerator.MoveNext ());
                    _setStatus (true);
                    break;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    // disconnect on error
                    Close ();
                    // delay in case of network error
                    System.Threading.Thread.Sleep (Details.RetryWaitMs);
                }
            }

            return Status == FileTranferStatus.Success;
        }

        public bool SendFile (string localFilename)
        {
            return SendFile (localFilename, System.IO.Path.Combine (Details.FilePath, System.IO.Path.GetFileName (localFilename)));
        }

        public bool SendFile (string localFilename, string destFilename)
        {
            using (var file = new FileStream (localFilename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultReadBufferSize))
            {
                return SendFile (file, destFilename, false);
            }
        }

        public bool SendFile (Stream localFile, string destFullPath, bool closeInputStream)
        {
            // try to upload file
            for (int i = 0; i < Details.RetryCount; i++)
            {
                // try to open
                if (!Open ())
                    return false;
                try
                {
                    // upload
                    using (Stream ostream = new S3UploadStream (client, destFullPath, Details.Get ("UseReducedRedundancy", false), true, Details.Get ("MakePublic", false), Details.Get ("PartSize", 10 * 1024 * 1024)))
                    {
                        using (var file = localFile)
                        {
                            file.CopyTo (ostream, FileTransferDetails.DefaultWriteBufferSize >> 2);
                        }
                    }                    
                    _setStatus (true);
                    return true;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    // disconnect on error
                    Close ();
                }
                finally
                {
                    if (closeInputStream)
                        localFile.Close();
                }
            }
            return false;
        }
    }
}