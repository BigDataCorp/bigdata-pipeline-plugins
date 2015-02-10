using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Amazon.S3;
using Amazon.S3.Model;
using System.Collections.Concurrent;
using System.Threading;

namespace AWSRedshiftPlugin
{
    /// <summary>
    /// 
    /// </summary>
    public class AWSS3Helper : IDisposable
    {
        private const int defaultStreamBufferSize = 2 * 1024 * 1024;

        private IAmazonS3 _client = null;
        private readonly string _accessKeyID;
        private readonly string _secretAccessKeyID;
        private readonly string _bucketName;
        private readonly Amazon.RegionEndpoint _awsRegion;

        /// <summary>
        /// Gets or sets the last error.
        /// </summary>
        /// <value>The last error.</value>
        public string LastError { get; set; }

        /// <summary>
        /// Gets the has error.
        /// </summary>
        /// <value>The has error.</value>
        public bool HasError
        {
            get { return LastError != null; }
        }

        /// <summary>
        /// Gets the aws region.
        /// </summary>
        /// <value>The aws region.</value>
        public Amazon.RegionEndpoint AwsRegion
        {
            get { return this._awsRegion; }
        }
        
        /// <summary>
        /// Gets the client.
        /// </summary>
        /// <value>The client.</value>
        public IAmazonS3 Client
        {
            get { return this._client; }
        }

        /// <summary>
        /// Gets the name of the bucket.
        /// </summary>
        /// <value>The name of the bucket.</value>
        public string BucketName
        {
            get { return this._bucketName; }
        }

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="AWSS3Helper" /> class.
        /// The USEast1 AWS region is used by default.
        /// </summary>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="bucketName">Name of the bucket.</param>
        public AWSS3Helper (string accessKeyID, string secretAccessKeyID, string bucketName) 
            : this (accessKeyID, secretAccessKeyID, bucketName, Amazon.RegionEndpoint.USEast1, true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AWSS3Helper" /> class.
        /// </summary>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="bucketName">Name of the bucket.</param>
        /// <param name="throwOnError">The throw an exception in case of errors.</param>
        public AWSS3Helper (string accessKeyID, string secretAccessKeyID, string bucketName, bool throwOnError) 
            : this (accessKeyID, secretAccessKeyID, bucketName, Amazon.RegionEndpoint.USEast1, throwOnError)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AWSS3Helper" /> class.
        /// </summary>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="bucketName">Name of the bucket.</param>
        /// <param name="region">AWS region of the S3 bucket.</param>
        public AWSS3Helper (string accessKeyID, string secretAccessKeyID, string bucketName, Amazon.RegionEndpoint region) 
            : this (accessKeyID, secretAccessKeyID, bucketName, region, true)
        { 
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AWSS3Helper" /> class.
        /// </summary>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="bucketName">Name of the bucket.</param>
        /// <param name="region">AWS region of the S3 bucket.</param>
        /// <param name="throwOnError">The throw an exception in case of errors.</param>
        public AWSS3Helper (string accessKeyID, string secretAccessKeyID, string bucketName, Amazon.RegionEndpoint region, bool throwOnError)
        { 
            _awsRegion = region;
            _accessKeyID = accessKeyID;
            _secretAccessKeyID = secretAccessKeyID;
            _bucketName = bucketName;

            //Open Bucket
            this.Open (throwOnError);            
        }

        private bool Open (bool throwOnError)
        {
            LastError = null;
            if (_client != null)
                return true;
            try
            {
                _client = Amazon.AWSClientFactory.CreateAmazonS3Client (_accessKeyID, _secretAccessKeyID, _awsRegion);
                return _client != null;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        private static bool CheckStatusCode (System.Net.HttpStatusCode code)
        {
            return ((int)code >= 200 && (int)code < 300);
        }

        private bool CanTryAgain (AmazonS3Exception ex)
        {
            if (ex.ErrorCode != null)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.Forbidden || ex.StatusCode == System.Net.HttpStatusCode.BadRequest ||
                    ex.StatusCode == System.Net.HttpStatusCode.NotFound || ex.StatusCode == System.Net.HttpStatusCode.Conflict)
                    return false;
                if (ex.ErrorCode == "SlowDown" || ex.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable)
                    Thread.Sleep (400);
            }
            // else can try again
            return true;
        }

        private static string PrepareS3KeyStyle (string fullFilePath)
        {
            if (String.IsNullOrEmpty (fullFilePath))
                return fullFilePath;
            fullFilePath = fullFilePath.Replace ('\\', '/').Replace ("//", "/");
            if (fullFilePath.StartsWith ("/") && fullFilePath.Length > 0)
                fullFilePath = fullFilePath.Substring (1);
            return fullFilePath;
        }

        private static string ReplaceBaseFolder (string fullFilePath, string currentBaseFolder, string newBaseFolder)
        {
            // normalize paths
            fullFilePath = "/" + PrepareS3KeyStyle (fullFilePath);
            currentBaseFolder = "/" + PrepareS3KeyStyle (currentBaseFolder);
            newBaseFolder = "/" + PrepareS3KeyStyle (newBaseFolder);

            if (!currentBaseFolder.EndsWith ("/"))
                currentBaseFolder += "/";
            if (!newBaseFolder.EndsWith ("/"))
                newBaseFolder += "/";

            // try to replace
            int pos = fullFilePath.IndexOf (currentBaseFolder, StringComparison.Ordinal);
            if (pos != 0)
            {
                return PrepareS3KeyStyle (fullFilePath);
            }
            return PrepareS3KeyStyle (newBaseFolder + fullFilePath.Substring (pos + currentBaseFolder.Length));
        }

        #endregion
        
        /// <summary>
        /// Returns a list of lifecycle configuration rules
        /// </summary>
        public LifecycleConfiguration GetLifecycleConfiguration (bool throwOnError = false)
        {
            try
            {
                // Retrieve lifecycle configuration.
                GetLifecycleConfigurationRequest request = new GetLifecycleConfigurationRequest
                {
                    BucketName = _bucketName
                };
                var response = _client.GetLifecycleConfiguration (request);
                if (!CheckStatusCode (response.HttpStatusCode))
                    throw new Exception (response.HttpStatusCode.ToString ());
                return response.Configuration;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            
            return null;
        }
        
        /// <summary>
        /// Adds a Lifecycle configuration with just one roule in the bucket. If already exists any configuration in the Bucket, it will be replaced.
        /// You should only use this method if there isn't an life cycle configuration in the bucket.
        /// </summary>
        /// <param name="Id">An Id for the Rule</param>
        /// <param name="prefix">An prefix to identify the files where the rule should be applied</param>
        /// <param name="expiration">Number of days until the file expirate</param>
        /// <param name="status">A Bollean that indicates if the rule is activated or not</param>
        /// <param name="throwOnError">throws exception on error</param>
        /// <returns></returns>
        private bool AddLifeCycleConfig (string id, string prefix, int expiration, bool status = true, bool throwOnError = false)
        {
            try
            {
                var rule = new LifecycleRule ()
                {
                    Id = id,
                    Prefix = prefix,
                    Status = status ? LifecycleRuleStatus.Enabled : LifecycleRuleStatus.Disabled,
                    Expiration = new LifecycleRuleExpiration
                    {
                        Days = expiration
                    }
                };
                // Add a sample configuration
                var lifeCycleConfiguration = new LifecycleConfiguration ()
                {
                    Rules = new List<LifecycleRule>
                    {
                        rule
                    }
                };
                
                PutLifeCycleConfigurations (lifeCycleConfiguration);
                
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Adds a new LifeCycleRule to a bucket. If the bucket doesn't have a LifeCycle configuration, its creates one with the rule.
        /// </summary>
        /// <param name="Id">An Id for the Rule</param>
        /// <param name="prefix">An prefix to identify the files where the rule should be applied</param>
        /// <param name="expiration">Number of days until the file expirate</param>
        /// <param name="status">A Bollean that indicates if the rule is activated or not</param>
        /// <param name="throwOnError">throws exception on error</param>
        /// <returns></returns>
        public bool AddLifeCycleRule (string id, string prefix, int expiration, bool status = true, bool throwOnError = false)
        {
            try
            {
                LifecycleConfiguration config = GetLifecycleConfiguration ();
                if (config != null)
                {
                    //Create Rule
                    var rule = new LifecycleRule
                    {
                        Id = id,
                        Prefix = prefix,
                        Status = status ? LifecycleRuleStatus.Enabled : LifecycleRuleStatus.Disabled,
                        Expiration = new LifecycleRuleExpiration { Days = expiration }
                    };
                    
                    // Add new rule.
                    config.Rules.Add (rule);
                    
                    PutLifeCycleConfigurations (config);
                    
                    return true;
                }
                else
                {
                    return AddLifeCycleConfig (id, prefix, expiration, status, throwOnError);
                }
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Deletes a LifeCycleConfiguration to a bucket
        /// </summary>
        public bool DeleteLifecycleConfiguration (bool throwOnError = false)
        {
            try
            {
                DeleteLifecycleConfigurationRequest request = new DeleteLifecycleConfigurationRequest
                {
                    BucketName = _bucketName
                };
                var response = _client.DeleteLifecycleConfiguration (request);
                
                if (!CheckStatusCode (response.HttpStatusCode))
                    throw new Exception (response.HttpStatusCode.ToString ());
                
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Remove Life Cycle Rule
        /// </summary>
        /// <param name="id"></param>
        /// <param name="throwOnError"></param>
        /// <returns></returns>
        public bool RemoveLifeCycleRule (string id, bool throwOnError = false)
        {
            try
            {
                LifecycleConfiguration config = GetLifecycleConfiguration ();
                
                // Add new rule.
                if (config != null)
                {
                    config.Rules.Remove (config.Rules.First (x => x.Id == id));
                    PutLifeCycleConfigurations (config);
                }
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Executes a Put Life Cycle Configuration Request
        /// </summary>
        /// <param name="config">LifeCycleConfiguration</param>
        private void PutLifeCycleConfigurations (LifecycleConfiguration config)
        {
            PutLifecycleConfigurationRequest request = new PutLifecycleConfigurationRequest
            {
                BucketName = _bucketName,
                Configuration = config
            };
            
            var response = _client.PutLifecycleConfiguration (request);
            
            if (!CheckStatusCode (response.HttpStatusCode))
                throw new Exception (response.HttpStatusCode.ToString ());
        }
        
        /// <summary>
        /// Updates aLife Cycle Rule
        /// </summary>
        /// <param name="Id">An Id for the Rule</param>
        /// <param name="prefix">An prefix to identify the files where the rule should be applied</param>
        /// <param name="expiration">Number of days until the file expirate</param>
        /// <param name="status">A Bollean that indicates if the rule is activated or not</param>
        /// <param name="throwOnError">throws exception on error</param>
        /// <returns></returns>
        public bool UpdateLifeCycleRule (string id, string prefix, int expiration, bool status = true, bool throwOnError = false)
        {
            try
            {
                LifecycleConfiguration config = GetLifecycleConfiguration ();
                
                // Add new rule.
                config.Rules.Remove (config.Rules.First (x => x.Id == id));
                
                //Create Rule
                var rule = new LifecycleRule
                {
                    Id = id,
                    Prefix = prefix,
                    Status = status ? LifecycleRuleStatus.Enabled : LifecycleRuleStatus.Disabled,
                    Expiration = new LifecycleRuleExpiration { Days = expiration }
                };
                
                // Add new rule.
                config.Rules.Add (rule);
                
                PutLifeCycleConfigurations (config);
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Checks if the bucket exists.
        /// </summary>
        /// <param name="bucketName">Name of the AWS S3 bucket.</param>
        public bool CheckBucketExistance (string bucketName, bool createIfNotExists, bool throwOnError = false)
        {
            try
            {
                Amazon.S3.Model.ListBucketsResponse response = _client.ListBuckets ();
                if (!CheckStatusCode (response.HttpStatusCode))
                    throw new Exception (response.HttpStatusCode.ToString ());
                
                // check bucket
                if (response.Buckets == null || !response.Buckets.Any (i => i.BucketName == bucketName))
                {
                    if (!createIfNotExists)
                        return false;
                    var r = _client.PutBucket (new PutBucketRequest ()
                    {
                        BucketName = bucketName,
                        BucketRegionName = _awsRegion.SystemName
                    });
                    if (!CheckStatusCode (r.HttpStatusCode))
                        throw new Exception (r.HttpStatusCode.ToString ());
                }
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        //http://docs.aws.amazon.com/AmazonS3/latest/dev/manage-lifecycle-using-dot-net.html
        /// <summary>
        /// Checks the bucket existance.
        /// </summary>
        /// <param name="bucketName">Name of the bucket.</param>
        /// <param name="expiration">The expiration.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool CheckBucketExistance (string bucketName, TimeSpan expiration, bool throwOnError = false)
        {
            if (!CheckBucketExistance (bucketName, true, throwOnError))
                return false;
            try
            {
                var config = GetLifecycleConfiguration (true);
                
                // NOT IMPLEMENTED
                throw new NotImplementedException ();
                
                // Add configuration
                var lifeCycleConfiguration = new LifecycleConfiguration ()
                {
                    Rules = new List<LifecycleRule>
                    {
                        new LifecycleRule
                        {
                            Id = "delete rule",
                            Status = LifecycleRuleStatus.Enabled,
                            Expiration = new LifecycleRuleExpiration ()
                            {
                                Days = (int)Math.Ceiling (expiration.TotalDays)
                            }
                        }
                    }
                };
                
                PutLifecycleConfigurationRequest request = new PutLifecycleConfigurationRequest
                {
                    BucketName = bucketName,
                    Configuration = lifeCycleConfiguration
                };
                
                var response = _client.PutLifecycleConfiguration (request);
                if (!CheckStatusCode (response.HttpStatusCode))
                    throw new Exception (response.HttpStatusCode.ToString ());
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Gets the file list for the bucket.
        /// </summary>
        /// <param name="recursive">If should list all folders recursively or only top folder.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public IEnumerable<string> GetFileList (bool recursive, bool throwOnError)
        {
            return GetFileList (null, recursive, throwOnError);
        }
        
        /// <summary>
        /// Gets the file list for the bucket.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="recursive">If should list all folders recursively or only top folder.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public IEnumerable<string> GetFileList (string folderPath, bool recursive, bool throwOnError, bool excludeFolders = true)
        {
            return ListFiles (folderPath, recursive, throwOnError, excludeFolders).Select (i => i.Key);
        }


        /// <summary>
        /// Lists the files.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="recursive">The recursive.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <param name="excludeFolders">The exclude folders.</param>
        /// <returns></returns>
        public IEnumerable<S3Object> ListFiles (string folderPath, bool recursive, bool throwOnError, bool excludeFolders)
        {
            var request = new ListObjectsRequest ()
            {
                BucketName = _bucketName,
                MaxKeys = 10000
            };

            folderPath = PrepareS3KeyStyle (folderPath);
            if (!String.IsNullOrEmpty (folderPath))
                request.Prefix = folderPath;

            if (!recursive)
                request.Delimiter = "/";

            int exceptionMarker = 0;
            string lastMarker = null;
            ListObjectsResponse response = null;
            do
            {
                if (lastMarker != null)
                    request.Marker = lastMarker;
                try
                {
                    response = _client.ListObjects (request);
                }
                catch (AmazonS3Exception ex)
                {
                    if (++exceptionMarker > 2 || !CanTryAgain (ex))
                    {
                        if (throwOnError)
                            throw ex;
                        else
                            break;
                    }
                    System.Threading.Thread.Sleep (100);
                }

                if (response != null)
                {
                    // process response
                    foreach (S3Object o in response.S3Objects)
                    {
                        if (excludeFolders && o.Key.EndsWith ("/"))
                            continue;
                        yield return o;
                    }

                    // clear last marker
                    lastMarker = null;
                    // If response is truncated, set the marker to get the next 
                    // set of keys.
                    if (response.IsTruncated)
                    {
                        lastMarker = response.NextMarker;
                    }
                    else
                    {
                        request = null;
                    }
                    // if we got here, clear exception counter
                    exceptionMarker = 0;
                }
            }
            while (request != null);
        }
        
        /// <summary>
        /// Reads the file.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public Stream ReadFile (string key, bool throwOnError)
        {
            GetObjectResponse response = null;
            try
            {
                key = PrepareS3KeyStyle (key);
                
                // prepare request
                response = _client.GetObject (new GetObjectRequest ()
                {
                    BucketName = _bucketName,
                    Key = key
                });
                return response.ResponseStream;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return null;
        }
        
        /// <summary>
        /// Reads the file lines.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="encoding">The encoding.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public IEnumerable<string> ReadFileLines (string key, System.Text.Encoding encoding, bool throwOnError)
        {
            var input = ReadFileStream (key, encoding, throwOnError);
            if (input == null)
                yield break;
            // download
            using (var reader = input)
            {
                string line = reader.ReadLine ();
                while (line != null)
                {
                    yield return line;
                    line = reader.ReadLine ();
                }
            }
        }
        
        /// <summary>
        /// Reads the file lines.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public IEnumerable<string> ReadFileLines (string key, bool throwOnError)
        {
            return ReadFileLines (key, System.Text.Encoding.GetEncoding ("ISO-8859-1"), throwOnError);
        }
        
        /// <summary>
        /// Reads the file as text.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="encoding">The encoding.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public string ReadFileAsText (string key, System.Text.Encoding encoding, bool throwOnError)
        {
            var input = ReadFileStream (key, encoding, throwOnError);
            if (input == null)
                return null;
            using (var reader = input)
            {
                return reader.ReadToEnd ();
            }
        }
        
        /// <summary>
        /// Reads the file as text.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public string ReadFileAsText (string key, bool throwOnError)
        {
            return ReadFileAsText (key, System.Text.Encoding.GetEncoding ("ISO-8859-1"), throwOnError);
        }
        
        /// <summary>
        /// Reads the file stream.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public StreamReader ReadFileStream (string key, bool throwOnError)
        {
            return ReadFileStream (key, System.Text.Encoding.GetEncoding ("ISO-8859-1"), throwOnError);
        }
        
        /// <summary>
        /// Reads the file stream.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="encoding">The encoding.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public StreamReader ReadFileStream (string key, System.Text.Encoding encoding, bool throwOnError)
        {
            var input = ReadFile (key, throwOnError);
            if (input == null)
                return null;
            // download
            return new StreamReader (input, encoding, true, 1 << 20);
        }

        /// <summary>
        /// Tries the get file.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="fileMask">The file mask.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="removeAfterDownload">The remove after download.</param>
        /// <returns></returns>
        public string TryGetFile (string folderPath, string fileMask, string destFolderPath, bool removeAfterDownload, bool throwOnError = false)
        {
            string finalFile = null;
            try
            {
                // search for file
                string exp = Regex.Escape (fileMask).Replace (@"\*", ".*").Replace (@"\?", ".");
                var reg = new Regex (exp, RegexOptions.IgnoreCase | RegexOptions.Singleline);
                string S3File = GetFileList (folderPath, true, true).FirstOrDefault (f => reg.IsMatch (System.IO.Path.GetFileName (f)));
                // download file if found                
                if (!String.IsNullOrEmpty (S3File))
                {
                    finalFile = System.IO.Path.Combine (destFolderPath, System.IO.Path.GetFileName (S3File));
                    // download file
                    if (DownloadFile (S3File, fileMask, destFolderPath, System.IO.Path.GetFileName (S3File), removeAfterDownload, throwOnError))
                        return finalFile;
                }
            }
            catch (Exception ex)
            {
                try
                {
                    // check for partial download
                    if (System.IO.File.Exists (finalFile))
                        System.IO.File.Delete (finalFile);
                }
                catch
                {
                }
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return null;
        }

        /// <summary>
        /// Downloads the file.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="destFullFilename">The dest full filename.</param>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="removeAfterDownload">The remove after download.</param>
        /// <returns></returns>
        public bool DownloadFile (string key, string destFullFilename, bool removeAfterDownload = false, bool throwOnError = false)
        {
            try
            {
                key = PrepareS3KeyStyle (key);
                
                // prepare request
                var request = new GetObjectRequest ()
                {
                    BucketName = _bucketName,
                    Key = key
                };

                // download
                using (GetObjectResponse response = _client.GetObject (request))
                {
                    if (!CheckStatusCode (response.HttpStatusCode))
                        throw new Exception (response.HttpStatusCode.ToString ());
                    // save to disk
                    using (var writer = new FileStream (destFullFilename, FileMode.CreateNew, FileAccess.Write, FileShare.None, defaultStreamBufferSize))
                    {
                        using (var reader = response.ResponseStream)
                        {
                            byte[] buffer = new byte[256 * 1024];
                            int n;
                            while ((n = reader.Read (buffer, 0, buffer.Length)) != 0)
                                writer.Write (buffer, 0, n);
                        }
                    }
                }
                
                // remove data from S3 if requested
                if (removeAfterDownload)
                {
                    DeleteFile (key, true);                    
                }
                
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Downloads the file.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="filename">The filename.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="destFilename">The dest filename.</param>
        /// <param name="removeAfterDownload">The remove after download.</param>
        /// <returns></returns>
        public bool DownloadFile (string folderPath, string filename, string destFolderPath, string destFilename, bool removeAfterDownload = false, bool throwOnError = false)
        {
            string key = System.IO.Path.Combine (PrepareS3KeyStyle (folderPath), PrepareS3KeyStyle (filename));
            return DownloadFile (key, System.IO.Path.Combine (destFolderPath, System.IO.Path.GetFileName (destFilename)), removeAfterDownload, throwOnError);
        }

        /// <summary>
        /// The method saves the string "datavalue" associated with a key "dataname" into the bucket "bucketname"
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <param name="makePublic">The make public.</param>
        /// <returns></returns>
        public bool UploadString (string key, string value, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false)
        {
            try
            {
                key = PrepareS3KeyStyle (key);

                var p = new PutObjectRequest ()
                {
                    BucketName = _bucketName,
                    Key = key,
                };

                p.ContentType = "text/plain";
                p.ContentBody = value;

                if (makePublic)
                    p.CannedACL = S3CannedACL.PublicRead;

                p.StorageClass = useReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;

                var response = _client.PutObject (p);

                return CheckStatusCode (response.HttpStatusCode);
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }

        /// <summary>
        /// The method saves the (image) stream "dataStream" associated with a key "dataname" into the bucket "bucketname"
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="key">The key.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <param name="makePublic">The make public.</param>
        /// <returns></returns>
        public bool UploadImageStream (Stream stream, string key, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false)
        {
            return UploadFile (stream, key, useReducedRedundancy, throwOnError, makePublic, "image/" + Path.GetExtension (key).Replace (".", ""));
        }

        /// <summary>
        /// Uploads the file.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="key">The key.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <param name="makePublic">The make public.</param>
        /// <returns></returns>
        public bool UploadFile (Stream stream, string key, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false, string contentType = null)
        {
            try
            {
                CheckBucketExistance (_bucketName, true, true);
                
                key = PrepareS3KeyStyle (key);
                
                var p = new PutObjectRequest ()
                {
                    BucketName = _bucketName,
                    Key = key,
                };
                p.InputStream = stream;

                if (!String.IsNullOrEmpty (contentType))
                    p.ContentType = contentType;
                if (makePublic)
                    p.CannedACL = S3CannedACL.PublicRead;
                
                p.StorageClass = useReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;
                
                var response = _client.PutObject (p);
                
                return CheckStatusCode (response.HttpStatusCode);
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }

        /// <summary>
        /// Uploads the file.
        /// </summary>
        /// <param name="fullFilename">The full filename.</param>
        /// <param name="key">The key.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <param name="makePublic">The make public.</param>
        /// <returns></returns>
        public bool UploadFile (string fullFilename, string key, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false)
        {
            const int multipartThreshold = 250 * 1024 * 1024;
            const int multipartSize = 100 * 1024 * 1024;
            try
            {
                using (var stream = new FileStream (fullFilename, FileMode.Open, FileAccess.Read, FileShare.Read, defaultStreamBufferSize))
                {
                    var contentLength = stream.Length;
                    stream.Position = 0;
                    if (contentLength < multipartThreshold)
                        return UploadFile (stream, key, useReducedRedundancy, throwOnError, makePublic);
                    else
                        return UploadFileAsMultipart (stream, key, useReducedRedundancy, throwOnError, makePublic, multipartSize);
                }
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }

        /// <summary>
        /// Uploads the file.
        /// </summary>
        /// <param name="fullFilename">The full filename.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="destFilename">The dest filename.</param>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        public bool UploadFile (string fullFilename, string destFolderPath, string destFilename, bool useReducedRedundancy, bool throwOnError = false)
        {
            string key = System.IO.Path.Combine (PrepareS3KeyStyle (destFolderPath), PrepareS3KeyStyle (destFilename));
            return UploadFile (fullFilename, key, useReducedRedundancy, throwOnError);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="filePath"></param>
        /// <param name="useReducedRedundancy"></param>
        /// <returns></returns>
        public bool UploadFileAsMultipart (Stream stream, string key, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false, long partSize = 150 * 1024 * 1024)
        {
            // try to upload data
            try
            {
                MultipartUploadStart (key, useReducedRedundancy, makePublic, true);

                // 2. Upload Parts.
                long filePosition = 0;
                var contentLength = stream.Length;//new FileInfo (filePath).Length;
                stream.Position = 0;
                while (filePosition < contentLength)
                {
                    MultipartUploadPart (stream, (int)partSize, true);                    

                    filePosition += partSize;
                }

                // Step 3: Complete.
                return MultipartUploadFinish (true);
            }
            catch (Exception ex)
            {                
                LastError = ex.Message;
                MultipartUploadAbort ();
                if (throwOnError)
                    throw ex;
            }
            return false;
        }

        private class MultipartUploadInfo
        {
            public List<UploadPartResponse> uploadResponses = new List<UploadPartResponse> ();
            public InitiateMultipartUploadResponse initResponse;
            public string key;
            public int partId = 1;
        }

        MultipartUploadInfo _uploadInfo = null;

        public bool MultipartUploadStart (string key, bool useReducedRedundancy, bool makePublic, bool throwOnError = false)
        {
            _uploadInfo = new MultipartUploadInfo ();

            // try to upload data
            try
            {
                key = PrepareS3KeyStyle (key);
                _uploadInfo.key = key;

                // 1. prepare parameters
                InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest
                {
                    BucketName = BucketName,
                    Key = key
                };

                // options
                if (useReducedRedundancy)
                    initiateRequest.StorageClass = S3StorageClass.ReducedRedundancy;
                if (makePublic)
                    initiateRequest.CannedACL = S3CannedACL.PublicRead;

                _uploadInfo.initResponse = _client.InitiateMultipartUpload (initiateRequest);

                return CheckStatusCode (_uploadInfo.initResponse.HttpStatusCode);
            }
            catch (Exception ex)
            {
                MultipartUploadAbort ();
                LastError = ex.Message;
                if (throwOnError)
                    throw ex;
            }
            return false;
        }

        public void MultipartUploadAbort ()
        {
            if (_uploadInfo != null && _uploadInfo.initResponse != null)
            {
                var abortRequest = new AbortMultipartUploadRequest
                {
                    BucketName = BucketName,
                    Key = _uploadInfo.key,
                    UploadId = _uploadInfo.initResponse.UploadId
                };
                for (var i = 0; i < 3; i++)
                {
                    try
                    {
                        _client.AbortMultipartUpload (abortRequest);
                        return;
                    }  catch { }
                    Thread.Sleep (1000);
                }
                // also abort old operations
                
            }
            _uploadInfo = null;
            try
            {
                var transferUtility = new Amazon.S3.Transfer.TransferUtility (_client);
                // Aborting uploads that were initiated over a week ago.
                transferUtility.AbortMultipartUploads (_bucketName, DateTime.UtcNow.AddDays (-7));
            }
            catch { }
        }

        public bool MultipartUploadPart (Stream stream, int partSize, bool throwOnError = false)
        {
            // try to upload data
            try
            {                
                // 2. Upload Parts.                                
                // Create a request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest
                {
                    BucketName = BucketName,
                    Key = _uploadInfo.key,
                    UploadId = _uploadInfo.initResponse.UploadId,
                    PartNumber = _uploadInfo.partId++,
                    PartSize = partSize,
                    InputStream = stream
                };

                // Upload part and add response to our list.
                UploadPartResponse resp = null;

                // try to upload with 3 retries
                for (var i = 0; i < 3; i++)
                {
                    try
                    {
                        resp = _client.UploadPart (uploadRequest);
                        if (CheckStatusCode (resp.HttpStatusCode))
                            break;
                        else
                            throw new Exception (resp.HttpStatusCode.ToString ());
                    }
                    catch
                    {
                        if (i == 2)
                            throw;
                    }
                }
                _uploadInfo.uploadResponses.Add (resp);
                return CheckStatusCode (resp.HttpStatusCode);
            }
            catch (Exception ex)
            {
                MultipartUploadAbort ();
                LastError = ex.Message;
                if (throwOnError)
                    throw ex;
            }
            return false;
        }

        public bool MultipartUploadFinish (bool throwOnError = false)
        {
            // try to upload data
            try
            {
                // Step 3: Complete.
                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = BucketName,
                    Key = _uploadInfo.key,
                    UploadId = _uploadInfo.initResponse.UploadId
                };
                completeRequest.AddPartETags (_uploadInfo.uploadResponses);

                CompleteMultipartUploadResponse completeUploadResponse = _client.CompleteMultipartUpload (completeRequest);

                return CheckStatusCode (completeUploadResponse.HttpStatusCode);
            }
            catch (Exception ex)
            {
                MultipartUploadAbort ();
                LastError = ex.Message;
                if (throwOnError)
                    throw ex;                
            }
            return false;
        }
        
        /// <summary>
        /// Deletes the file.
        /// </summary>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="destFilename">The dest filename.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool DeleteFile (string destFolderPath, string destFilename, bool throwOnError = false)
        {
            string key = System.IO.Path.Combine (PrepareS3KeyStyle (destFolderPath), PrepareS3KeyStyle (destFilename));
            return DeleteFile (key, throwOnError);
        }
        
        /// <summary>
        /// Deletes the file.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool DeleteFile (string filename, bool throwOnError = false)
        {
            try
            {
                if (!CheckBucketExistance (_bucketName, false, true))
                    return true;
                
                string key = PrepareS3KeyStyle (filename);
                
                var p = new DeleteObjectRequest ();
                p.BucketName = _bucketName;
                p.Key = key;
                var response = DeleteItem (p);
                
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        private DeleteObjectResponse DeleteItem (DeleteObjectRequest request)
        {
            // try to put the acl of the object
            DeleteObjectResponse response = null;
            int i = 0;
            while (response == null)
            {
                try
                {
                    response = _client.DeleteObject (request);
                    return response;
                }
                catch (AmazonS3Exception ex)
                {
                    if (++i > 2 || !CanTryAgain (ex))
                        throw ex;
                }
            }
            return response;
        }

        public bool DeleteFiles (IList<string> keys, bool throwOnError = false)
        {
            try
            {
                int maxTries = ((int)(keys.Count / 1000)) + 2;
                int tryCount = 0;
                int errorCounter = 0;
                while (keys != null && keys.Count > 0 && tryCount++ < maxTries)
                {
                    var p = new DeleteObjectsRequest ();
                    p.BucketName = _bucketName;
                    p.Quiet = true;
                    foreach (var key in keys.Take (999))
                        p.AddKey (PrepareS3KeyStyle (key));                    
                    
                    try
                    {
                        var response = _client.DeleteObjects (p);

                        keys = keys.Skip (999)
                            .Concat (response.DeleteErrors.Select (i => i.Key)).ToArray ();
                    }
                    catch (AmazonS3Exception ex)
                    {
                        if (++errorCounter > 3)
                            throw ex;
                        continue;
                    }

                    errorCounter = 0;
                }
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Deletes Folder files and subfolders in the path.
        /// </summary>
        public bool DeleteFolder (string folderPath, bool throwOnError = false)
        {
            try
            {
                IEnumerable<string> filesList = GetFileList (folderPath, true, true, false);
                bool res = true;
                var list = filesList.Take (990).ToList ();
                while (list != null && list.Count > 0)
                {
                    DeleteFiles (list, true);
                    list = filesList.Take (990).ToList ();
                }
                return res;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Copies the folder.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool CopyFolder (string folderPath, string destFolderPath, bool throwOnError = false, bool makePublic = false)
        {
            try
            {
                folderPath = PrepareS3KeyStyle (folderPath);
                if (!folderPath.EndsWith ("/"))
                    folderPath += "/";
                IEnumerable<string> filesList = GetFileList (folderPath, true, throwOnError);
                bool res = true;
                foreach (var item in filesList)
                {
                    string i = item.Substring (folderPath.Length);
                    bool copied = CopyFile (folderPath, i, destFolderPath, i, throwOnError, makePublic);
                    if (copied == false)
                    {
                        res = false;
                        break;
                    }
                }
                return res;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Copies the file.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="filename">The filename.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="destFileName">Name of the dest file.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool CopyFile (string folderPath, string filename, string destFolderPath, string destFileName, bool throwOnError = false, bool makePublic = false)
        {
            string key = PrepareS3KeyStyle (System.IO.Path.Combine (PrepareS3KeyStyle (folderPath), PrepareS3KeyStyle (filename)));
            string newKey = PrepareS3KeyStyle (System.IO.Path.Combine (PrepareS3KeyStyle (destFolderPath), PrepareS3KeyStyle (destFileName)));
            return CopyFile (key, newKey, throwOnError, makePublic);
        }
        
        /// <summary>
        /// Copies the file.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <param name="destFileName">Name of the dest file.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool CopyFile (string filename, string destFileName, bool throwOnError = false, bool makePublic = false)
        {
            try
            {
                CheckBucketExistance (_bucketName, true, true);
                
                string newKey = PrepareS3KeyStyle (destFileName);
                string key = PrepareS3KeyStyle (filename);
                
                // copy the object without acl
                CopyObjectRequest copyRequest = new CopyObjectRequest ()
                {
                    SourceBucket = _bucketName,
                    DestinationBucket = _bucketName,
                    SourceKey = key,
                    DestinationKey = newKey
                };
                
                if (makePublic)
                    copyRequest.CannedACL = S3CannedACL.PublicRead;
                
                CopyObjectResponse copyResponse = CopyItem (copyRequest);
                
                // get the acl of the object
                if (!makePublic)
                {                
                    GetACLResponse getAclResponse = GetItemACL (key);
                    
                    // set the acl of the newly created object
                    PutACLRequest setAclRequest = new PutACLRequest ()
                    {
                        BucketName = _bucketName,
                        Key = newKey,
                        AccessControlList = getAclResponse.AccessControlList
                    };
                    
                    setAclRequest.AccessControlList = getAclResponse.AccessControlList;
                    
                    PutItemACL (setAclRequest);
                }
                
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        private GetACLResponse GetItemACL (string key)
        {
            // prepare the request
            GetACLRequest aclRequest = new GetACLRequest ()
            {
                BucketName = _bucketName,
                Key = key
            };
            // try to get the acl of the object
            GetACLResponse response = null;
            int i = 0;
            while (response == null)
            { 
                try
                {
                    response = _client.GetACL (aclRequest);
                    return response;
                }
                catch (AmazonS3Exception ex)
                {
                    if (++i > 2 || !CanTryAgain (ex))
                        throw ex;
                }
            }
            return response;
        }
        
        private void PutItemACL (PutACLRequest request)
        { 
            // try to put the acl of the object
            PutACLResponse response = null;
            int i = 0;
            while (response == null)
            {
                try
                {
                    response = _client.PutACL (request);
                    return;
                }
                catch (AmazonS3Exception ex)
                {
                    if (++i > 2 || !CanTryAgain (ex))
                        throw ex;
                }
            }
        }
        
        private CopyObjectResponse CopyItem (CopyObjectRequest request)
        {
            // try to put the acl of the object
            CopyObjectResponse response = null;
            int i = 0;
            while (response == null)
            {
                try
                {
                    response = _client.CopyObject (request);
                    return response;
                }
                catch (AmazonS3Exception ex)
                {
                    if (++i > 2 || !CanTryAgain (ex))
                        throw ex;
                }
            }
            return response;
        }
        
        /// <summary>
        /// Moves the folder.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool MoveFolder (string folderPath, string destFolderPath, bool throwOnError = false)
        {
            try
            {
                folderPath = PrepareS3KeyStyle (folderPath);
                
                IEnumerable<string> filesList = GetFileList (folderPath, true, true);
                bool res = true;
                foreach (var item in filesList)
                {
                    string destKey = ReplaceBaseFolder (item, folderPath, destFolderPath);
                    bool copied = MoveFile (item, destKey, true);
                    if (copied == false)
                    {
                        res = false;
                        break;
                    }
                }
                return res;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }

        /// <summary>
        /// Moves the file.
        /// </summary>
        /// <param name="folderPath">The folder path.</param>
        /// <param name="filename">The filename.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="destFileName">Name of the dest file.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool MoveFile (string folderPath, string filename, string destFolderPath, string destFileName, bool throwOnError = false)
        {
            try
            {
                CopyFile (folderPath, filename, destFolderPath, destFileName, true);
                DeleteFile (folderPath, filename, true);
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Moves the file.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <param name="destFileName">Name of the dest file.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <returns></returns>
        public bool MoveFile (string filename, string destFileName, bool throwOnError = false)
        {
            try
            { 
                // retry again
                CopyFile (filename, destFileName, true);
                DeleteFile (filename, true);                
                return true;
            }
            catch (Exception ex)
            {
                if (throwOnError)
                    throw ex;
                LastError = ex.Message;
            }
            return false;
        }
        
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing,
        /// or resetting unmanaged resources.
        /// </summary>
        public void Dispose ()
        {
            if (_client != null)
                _client.Dispose ();
            _client = null;
        }
    }



    public class S3UploadStream : Stream
    {
        #region overrides without any interested

        /// <summary>
        /// Sets the position within the current stream.
        /// </summary>
        /// <param name="offset">A byte offset relative to the <paramref name="origin" /> parameter.</param>
        /// <param name="origin">A value of type <see cref="T:System.IO.SeekOrigin" /> indicating the reference point used to obtain the new position.</param>
        /// <exception cref="T:System.IO.IOException">
        /// An I/O error occurs.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support seeking, such as if the stream is constructed from a pipe or console output.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">
        /// Methods were called after the stream was closed.
        /// </exception>
        /// <returns>The new position within the current stream.</returns>
        public override long Seek (long offset, SeekOrigin origin)
        {
            throw new NotSupportedException ();
        }

        /// <summary>
        /// Sets the length of the current stream.
        /// </summary>
        /// <param name="value">The desired length of the current stream in bytes.</param>
        /// <exception cref="T:System.IO.IOException">An I/O error occurs. </exception>
        /// <exception cref="T:System.NotSupportedException">The stream does not support
        /// both writing and seeking, such as if the stream is constructed from a pipe or
        /// console output. </exception>
        /// <exception cref="T:System.ObjectDisposedException">Methods were called after
        /// the stream was closed. </exception>
        public override void SetLength (long value)
        {
            
        }

        /// <summary>
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read.
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between <paramref name="offset" /> and (<paramref name="offset" /> + <paramref name="count" /> - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <exception cref="T:System.ArgumentException">
        /// The sum of <paramref name="offset" /> and <paramref name="count" /> is larger than the buffer length.
        /// </exception>
        /// <exception cref="T:System.ArgumentNullException">
        /// <paramref name="buffer" /> is null.
        /// </exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        /// <paramref name="offset" /> or <paramref name="count" /> is negative.
        /// </exception>
        /// <exception cref="T:System.IO.IOException">
        /// An I/O error occurs.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support reading.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">
        /// Methods were called after the stream was closed.
        /// </exception>
        /// <returns>
        /// The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.
        /// </returns>
        public override int Read (byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException ();
        }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports reading.
        /// </summary>
        /// <returns>true if the stream supports reading; otherwise, false.
        /// </returns>
        /// <value></value>
        public override bool CanRead
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value indicating whether the current stream supports seeking.
        /// </summary>
        /// <returns>true if the stream supports seeking; otherwise, false.
        /// </returns>
        /// <value></value>
        public override bool CanSeek
        {
            get { return false; }
        }
                
        /// <summary>
        /// Gets the length in bytes of the stream.
        /// </summary>
        /// <returns>
        /// A long value representing the length of the stream in bytes.
        /// </returns>
        /// <exception cref="T:System.NotSupportedException">
        /// A class derived from Stream does not support seeking.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">
        /// Methods were called after the stream was closed.
        /// </exception>
        /// <value></value>
        public override long Length
        {
            get { throw new NotSupportedException (); }
        }

        /// <summary>
        /// Gets or sets the position within the current stream.
        /// </summary>
        /// <returns>
        /// The current position within the stream.
        /// </returns>
        /// <exception cref="T:System.IO.IOException">
        /// An I/O error occurs.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support seeking.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">
        /// Methods were called after the stream was closed.
        /// </exception>
        /// <value></value>
        public override long Position
        {
            get { throw new NotSupportedException (); }
            set { throw new NotSupportedException (); }
        }

        #endregion

        /// <summary>
        /// Gets a value indicating whether the current stream supports writing.
        /// </summary>
        /// <returns>true if the stream supports writing; otherwise, false.
        /// </returns>
        /// <value></value>
        public override bool CanWrite
        {
            get { return true; }
        }
                
        /// <summary>
        /// Write buffer
        /// </summary>
        private readonly MemoryStream m_writeBuffer;

        /// <summary>
        /// Current position in write buffer
        /// </summary>
        private int _writeBufferOffset;

        /// <summary>
        /// Clears all buffers for this stream and causes any buffered data to be written to the underlying device.
        /// </summary>
        /// <exception cref="T:System.IO.IOException">
        /// An I/O error occurs.
        /// </exception>
        public override void Flush ()
        {
            // do nothing ... (we must respect s3 part size)
        }

        private void internalFlush()
        {
            if (_writeBufferOffset > 0 && _client != null)
            {
                _writeBufferOffset = 0;
                m_writeBuffer.Position = 0;
                _client.MultipartUploadPart(m_writeBuffer, (int)m_writeBuffer.Length, _throwOnError);
                m_writeBuffer.SetLength(0);
            }
        }

        /// <summary>
        /// Writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
        /// </summary>
        /// <param name="buffer">An array of bytes. This method copies <paramref name="count" /> bytes from <paramref name="buffer" /> to the current stream.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin copying bytes to the current stream.</param>
        /// <param name="count">The number of bytes to be written to the current stream.</param>
        /// <exception cref="T:System.ArgumentException">
        /// The sum of <paramref name="offset" /> and <paramref name="count" /> is greater than the buffer length.
        /// </exception>
        /// <exception cref="T:System.ArgumentNullException">
        /// <paramref name="buffer" /> is null.
        /// </exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        /// <paramref name="offset" /> or <paramref name="count" /> is negative.
        /// </exception>
        /// <exception cref="T:System.IO.IOException">
        /// An I/O error occurs.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        /// The stream does not support writing.
        /// </exception>
        /// <exception cref="T:System.ObjectDisposedException">
        /// Methods were called after the stream was closed.
        /// </exception>
        public override void Write (byte[] buffer, int offset, int count)
        {
            // we have 3 options here:
            // buffer can still be filled --> we fill
            // buffer is full --> we flush
            // buffer is overflood --> we flush and refill

            // 1. there is enough room, the buffer is not full
            int lengthToCauseFlush = _partSize - _writeBufferOffset;
            if (count <= lengthToCauseFlush)
            {
                m_writeBuffer.Write (buffer, offset, count);                
                _writeBufferOffset += count;
                // 2. same size: write
                if (lengthToCauseFlush == 0)
                    internalFlush ();
            }
            // 3. buffer overflow: we split
            else
            {
                // this first Write will cause a flush
                Write (buffer, offset, lengthToCauseFlush);
                // this one will refill
                Write (buffer, offset + lengthToCauseFlush, count - lengthToCauseFlush);
            }
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="T:System.IO.Stream" /> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose (bool disposing)
        {
            if (disposing)
            {
                if (_client != null)
                {
					internalFlush ();
                    _client.MultipartUploadFinish (_throwOnError);
                }
                _client = null;
            }
            base.Dispose (disposing);
        }

        public void Dispose ()
        {
            Dispose (true);
        }

        string _key;
        bool _redundancy;
        bool _throwOnError;
        bool _public;
        int _partSize;
        AWSS3Helper _client;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZlibCompressionStream" /> class.
        /// </summary>
        /// <param name="targetStream">The target stream.</param>
        /// <param name="writeBuffer">The write buffer.</param>
        /// <param name="compressionBuffer">The compression buffer.</param>
        /// <param name="compression">The compression.</param>
        /// <param name="closeStream">The close stream.</param>
        public S3UploadStream (AWSS3Helper client, string key, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false, int partSize = 5 * 1024 * 1024)
        {
            _key = key;
            _redundancy = useReducedRedundancy;
            _throwOnError = throwOnError;
            _public = makePublic;
            _partSize = partSize;
            _client = client;
			// the part size must be at least 5Mb (s3 requeriment)
            if (_partSize < (int)(5.1 * 1024 * 1024))
                _partSize = (int)(5.1 * 1024 * 1024);
            if (_partSize > 250 * 1024 * 1024)
                _partSize = 250 * 1024 * 1024;
            m_writeBuffer = new MemoryStream (_partSize);
            _client.MultipartUploadStart (key, useReducedRedundancy, makePublic, throwOnError);
        }
    }

}