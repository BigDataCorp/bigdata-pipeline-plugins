﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileListenerPlugin
{
    public class FileSystemTransfer : IFileTransfer
    {

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
            return true;
        }

        private void CreateDirectory (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                return;
            folder = PreparePath (folder.Replace ('\\', '/'));
            FileTransferDetails.CreateDirectory (folder);
        }

        private string PreparePath (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                folder = "/";           
            return folder;
        }

        public bool IsOpened ()
        {
            return true;
        }

        public void Dispose ()
        {            
        }

        private IEnumerable<string> _listFiles (string folder, string pattern, bool recursive)
        {
            _setStatus (true);

            folder = PreparePath (folder);

            if (String.IsNullOrEmpty (pattern))
                pattern = "*";

            return System.IO.Directory.EnumerateFiles (folder, pattern, recursive ? System.IO.SearchOption.AllDirectories : System.IO.SearchOption.TopDirectoryOnly);
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
            return _listFiles (folder, fileMask, recursive);
        }

        public IEnumerable<StreamTransfer> GetFileStreams (string folder, string fileMask, bool recursive)
        {
            _setStatus (true);
            // download files
            foreach (var f in _listFiles (folder, fileMask, recursive))
            {
                yield return new StreamTransfer
                {
                    FileName = f,
                    FileStream = new FileStream (f, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultReadBufferSize)
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
                string newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
                FileTransferDetails.DeleteFile (newFile);

                try
                {
                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultWriteBufferSize))
                    {
                        f.FileStream.CopyTo (file, FileTransferDetails.DefaultWriteBufferSize >> 2);
                    }

                    // check if we must remove file
                    if (deleteOnSuccess)
                    {
                        FileTransferDetails.DeleteFile (f.FileName);
                    }

                    _setStatus (true);
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    FileTransferDetails.DeleteFile (newFile);
                    newFile = null;
                }
                finally
                {
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
            foreach (var f in files)
                FileTransferDetails.DeleteFile (f);

            _setStatus (true);

            return Status == FileTranferStatus.Success;
        }

        public bool SendFile (string localFilename)
        {
            return SendFile (localFilename, System.IO.Path.Combine (Details.FilePath, System.IO.Path.GetFileName (localFilename)));
        }

        public bool SendFile (string localFilename, string destFilename)
        {
            using (var file = new FileStream (localFilename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, 1024 * 1024))
            {
                return SendFile (file, destFilename, false);
            }
        }

        public bool SendFile (Stream localFile, string destFullPath, bool closeInputStream)
        {
            try
            {
                FileTransferDetails.CreateDirectory (System.IO.Path.GetDirectoryName (destFullPath));

                using (var file = new FileStream (destFullPath, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultWriteBufferSize))
                {
                    localFile.CopyTo (file, FileTransferDetails.DefaultWriteBufferSize >> 2);
                }
                _setStatus (true);
            }
            catch (Exception ex)
            {
                _setStatus (ex);
            }
            finally
            {
                if (closeInputStream)
                    localFile.Close ();
            }
            return Status == FileTranferStatus.Success;
        }
    }
}