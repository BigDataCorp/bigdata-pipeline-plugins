using System;
using System.Collections.Generic;
using System.IO;

namespace FileListenerPlugin
{
    public enum FileLocation { Unkown, FileSystem, FTP, FTPS, FTPES, SFTP, HTTP, S3 }

    public enum FileTranferStatus { None, Success, Error }

    public interface IFileTransfer : IDisposable
    {        
        string LastError { get; }

        FileTransferDetails Details { get; }

        FileTranferStatus Status { get; }

        /// <summary>
        /// 
        /// ftp://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// ftps://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// ftpes://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// sftp://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// s3://[login:password@][region endpoint]/[bucketname]/[path]/[file name or wildcard expression]
        /// s3://[login:password@][s3-us-west-1.amazonaws.com]/[bucketname]/[path]/[file name or wildcard expression]
        /// http://[server][:port]/[path]/[file name or wildcard expression]
        /// https://[server][:port]/[path]/[file name or wildcard expression]
        /// c:/[path]/[file name or wildcard expression]
        /// 
        /// </summary>
        bool Open (FileTransferDetails details);

        bool IsOpened ();

        void Dispose ();

        IEnumerable<FileTransferInfo> ListFiles ();

        IEnumerable<FileTransferInfo> ListFiles (string folder, bool recursive);

        IEnumerable<FileTransferInfo> ListFiles (string folder, string fileMask, bool recursive);

        StreamTransfer GetFileStream (string file);

        IEnumerable<StreamTransfer> GetFileStreams (string folder, string fileMask, bool recursive);

        IEnumerable<StreamTransfer> GetFileStreams ();

        FileTransferInfo GetFile (string file, string outputDirectory, bool deleteOnSuccess);

        IEnumerable<FileTransferInfo> GetFiles (string folder, string fileMask, bool recursive, string outputDirectory, bool deleteOnSuccess);

        IEnumerable<FileTransferInfo> GetFiles (string outputDirectory, bool deleteOnSuccess);

        bool RemoveFiles (IEnumerable<string> files);

        bool RemoveFile (string file);

        bool SendFile (string localFilename);

        bool SendFile (string localFilename, string destFilename);

        bool SendFile (Stream localFile, string destFullPath, bool closeInputStream);

        // TODO: get and remove | move
    }
}