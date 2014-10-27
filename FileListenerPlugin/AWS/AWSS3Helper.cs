using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Amazon.S3;
using Amazon.S3.Model;
using System.Collections.Concurrent;
using System.Threading;

namespace FileListenerPlugin
{
    public class AWSS3Helper : IDisposable
    {
        private IAmazonS3 _client = null;
        private readonly string _accessKeyID;
        private readonly string _secretAccessKeyID;
        private readonly string _bucketName;
        private readonly Amazon.RegionEndpoint _awsRegion;

        public string LastError { get; set; }

        public bool HasError
        {
            get { return LastError != null; }
        }

        public Amazon.RegionEndpoint AwsRegion
        {
            get
            {
                return this._awsRegion;
            }
        }
        
        public IAmazonS3 Client
        {
            get
            {
                return this._client;
            }
        }

        public string BucketName
        {
            get
            {
                return this._bucketName;
            }
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
            fullFilePath = fullFilePath.Replace ('\\', '/');
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
                var response = _client.GetLifecycleConfiguration(request);
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
        private bool AddLifeCycleConfig(string id, string prefix, int expiration ,bool status = true,bool throwOnError = false)
        {
            try
            {
                var rule = new LifecycleRule()
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
                var lifeCycleConfiguration = new LifecycleConfiguration()
                {
                    Rules = new List<LifecycleRule>
                    {
                        rule
                    }
                };

                PutLifeCycleConfigurations(lifeCycleConfiguration);
               
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
        public bool AddLifeCycleRule(string id, string prefix, int expiration ,bool status = true, bool throwOnError = false)
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
                    config.Rules.Add(rule);

                    PutLifeCycleConfigurations(config);

                    return true;
                }
                else
                {
                    return AddLifeCycleConfig(id, prefix, expiration, status, throwOnError);
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
        public bool DeleteLifecycleConfiguration(bool throwOnError = false)
        {
            try
            {
                DeleteLifecycleConfigurationRequest request = new DeleteLifecycleConfigurationRequest
                {
                    BucketName = _bucketName
                };
                var response = _client.DeleteLifecycleConfiguration(request);

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
        public bool RemoveLifeCycleRule(string id, bool throwOnError = false)
        {
            try
            {
                LifecycleConfiguration config = GetLifecycleConfiguration ();
                
                // Add new rule.
                if (config != null)
                {
                    config.Rules.Remove(config.Rules.First(x => x.Id == id));
                    PutLifeCycleConfigurations(config);
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
        private void PutLifeCycleConfigurations(LifecycleConfiguration config)
        {
            PutLifecycleConfigurationRequest request = new PutLifecycleConfigurationRequest
            {
                BucketName = _bucketName,
                Configuration = config
            };

            var response = _client.PutLifecycleConfiguration(request);
            
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
        public bool UpdateLifeCycleRule(string id, string prefix, int expiration, bool status = true, bool throwOnError = false)
        {
            try
            {
                LifecycleConfiguration config = GetLifecycleConfiguration ();
                
                // Add new rule.
                config.Rules.Remove(config.Rules.First(x=>x.Id==id));

                //Create Rule
                var rule = new LifecycleRule
                {
                    Id = id,
                    Prefix = prefix,
                    Status = status ? LifecycleRuleStatus.Enabled : LifecycleRuleStatus.Disabled,
                    Expiration = new LifecycleRuleExpiration { Days = expiration }
                };

                // Add new rule.
                config.Rules.Add(rule);

                PutLifeCycleConfigurations(config);
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
        public IEnumerable<string> GetFileList (string folderPath, bool recursive, bool throwOnError)
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
                        yield return o.Key;                    
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

        public IEnumerable<string> ReadFileLines (string key, bool throwOnError)
        {
            return ReadFileLines (key, System.Text.Encoding.GetEncoding ("ISO-8859-1"), throwOnError);
        }

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

        public string ReadFileAsText (string key, bool throwOnError)
        {
            return ReadFileAsText (key, System.Text.Encoding.GetEncoding ("ISO-8859-1"), throwOnError);
        }

        public StreamReader ReadFileStream (string key, bool throwOnError)
        {
            return ReadFileStream (key, System.Text.Encoding.GetEncoding ("ISO-8859-1"), throwOnError);
        }

        public StreamReader ReadFileStream (string key, System.Text.Encoding encoding, bool throwOnError)
        {
            var input = ReadFile (key, throwOnError);
            if (input == null)
                return null;
            // download
            return new StreamReader (input, encoding, true, 1 << 20);
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
        public bool DownloadFile(string key, string destFullFilename, bool removeAfterDownload = false, bool throwOnError = false)
        {
            try
            {
                key = PrepareS3KeyStyle (key);

                // prepare request
                var response = _client.GetObject (new GetObjectRequest ()
                {
                    BucketName = _bucketName,
                    Key = key
                });
                // download
                response.WriteResponseStreamToFile (destFullFilename, false);

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
        public bool DownloadFile(string folderPath, string filename, string destFolderPath, string destFilename, bool removeAfterDownload = false, bool throwOnError = false)
        {
            string key = System.IO.Path.Combine (PrepareS3KeyStyle (folderPath), PrepareS3KeyStyle (filename));
            return DownloadFile (key, System.IO.Path.Combine (destFolderPath, System.IO.Path.GetFileName (destFilename)), removeAfterDownload, throwOnError);
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
            try
            {
                CheckBucketExistance (_bucketName, true, true);

                key = PrepareS3KeyStyle (key);

                using (var stream = new FileStream(fullFilename, FileMode.Open))
                {
                    var p = new PutObjectRequest()
                    {
                        BucketName = _bucketName,
                        Key = key
                    };
                    p.InputStream = stream;
                    
                    if (makePublic)
                        p.CannedACL = S3CannedACL.PublicRead;
                        
                    p.StorageClass = useReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;

                    var response = _client.PutObject(p);
                
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
        /// Uploads the file.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="key">The key.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        /// <param name="throwOnError">The throw on error.</param>
        /// <param name="makePublic">The make public.</param>
        /// <returns></returns>
        public bool UploadFile (Stream stream, string key, bool useReducedRedundancy, bool throwOnError = false, bool makePublic = false)
        {
            try
            {
                CheckBucketExistance (_bucketName, true, true);

                key = PrepareS3KeyStyle (key);

                var p = new PutObjectRequest()
                {
                    BucketName = _bucketName,
                    Key = key,
                };
                p.InputStream = stream;
                if (makePublic)
                    p.CannedACL = S3CannedACL.PublicRead;
                    
                p.StorageClass = useReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;

                var response = _client.PutObject(p);

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
        /// Uploads the file.
        /// </summary>
        /// <param name="fullFilename">The full filename.</param>
        /// <param name="destFolderPath">The dest folder path.</param>
        /// <param name="destFilename">The dest filename.</param>
        /// <param name="accessKeyID">The access key ID.</param>
        /// <param name="secretAccessKeyID">The secret access key ID.</param>
        /// <param name="useReducedRedundancy">The use reduced redundancy.</param>
        public bool UploadFile(string fullFilename, string destFolderPath, string destFilename, bool useReducedRedundancy, bool throwOnError = false)
        {
            string key = System.IO.Path.Combine (PrepareS3KeyStyle (destFolderPath), PrepareS3KeyStyle (destFilename));
            return UploadFile(fullFilename, key, useReducedRedundancy, throwOnError);
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
        public string TryGetFile(string folderPath, string fileMask, string destFolderPath, bool removeAfterDownload, bool throwOnError = false)
        {
            string finalFile = null;
            try
            {
                // search for file
                string exp = Regex.Escape(fileMask).Replace(@"\*", ".*").Replace(@"\?", ".");
                var reg = new Regex(exp, RegexOptions.IgnoreCase | RegexOptions.Singleline);
                string S3File = GetFileList(folderPath, true, true).FirstOrDefault(f => reg.IsMatch(System.IO.Path.GetFileName(f)));
                // download file if found                
                if (!String.IsNullOrEmpty(S3File))
                {
                    finalFile = System.IO.Path.Combine(destFolderPath, System.IO.Path.GetFileName(S3File));
                    // download file
                    if (DownloadFile(S3File, fileMask, destFolderPath, System.IO.Path.GetFileName(S3File), removeAfterDownload,throwOnError))
                        return finalFile;
                }
            }
            catch (Exception ex)
            {
                try
                {
                    // check for partial download
                    if (System.IO.File.Exists(finalFile))
                        System.IO.File.Delete(finalFile);
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

        public bool DeleteFile(string destFolderPath, string destFilename, bool throwOnError = false)
        {
            string key = System.IO.Path.Combine (PrepareS3KeyStyle (destFolderPath), PrepareS3KeyStyle (destFilename));
            return DeleteFile (key, throwOnError);
        }

        public bool DeleteFile(string filename, bool throwOnError = false)
        {
            try
            {
                if (!CheckBucketExistance (_bucketName, false, true))
                    return true;

                string key = PrepareS3KeyStyle (filename);

                var p = new DeleteObjectRequest();
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

        /// <summary>
        /// Deletes Folder files and subfolders in the path.
        /// </summary>
        public bool DeleteFolder(string folderPath, bool throwOnError = false)
        {
            try
            {
                IEnumerable<string> filesList = GetFileList(folderPath, true, true);
                bool res = true;
                foreach (var item in filesList)
                {
                    bool deleted = DeleteFile(folderPath, item, true);
                    if (deleted == false)
                    {
                        res = false;
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

        public bool CopyFolder(string folderPath, string destFolderPath, bool throwOnError = false)
        {
            try
            {
                IEnumerable<string> filesList = GetFileList(folderPath, true, true);
                bool res = true;
                foreach (var item in filesList)
                {
                    bool copied = CopyFile(folderPath, item, destFolderPath, item, true);
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

        public bool CopyFile(string folderPath, string filename, string destFolderPath, string destFileName, bool throwOnError = false)
        {
            string key = PrepareS3KeyStyle (System.IO.Path.Combine (PrepareS3KeyStyle (folderPath), PrepareS3KeyStyle (filename)));
            string newKey = PrepareS3KeyStyle (System.IO.Path.Combine (PrepareS3KeyStyle (destFolderPath), PrepareS3KeyStyle (destFileName)));
            return CopyFile (key, newKey, throwOnError);
        }

        public bool CopyFile(string filename, string destFileName, bool throwOnError = false)
        {
            try
            {
                CheckBucketExistance (_bucketName, true, true);

                string newKey = PrepareS3KeyStyle (destFileName);
                string key = PrepareS3KeyStyle (filename);

                // get the acl of the object
                GetACLResponse getAclResponse = GetItemACL (key);

                // copy the object without acl
                CopyObjectRequest copyRequest = new CopyObjectRequest ()
                {
                    SourceBucket = _bucketName,
                    DestinationBucket = _bucketName,
                    SourceKey = key,
                    DestinationKey = newKey
                };

                CopyObjectResponse copyResponse = CopyItem (copyRequest);

                // set the acl of the newly created object
                PutACLRequest setAclRequest = new PutACLRequest ()
                {
                    BucketName = _bucketName,
                    Key = newKey,
                    AccessControlList = getAclResponse.AccessControlList
                };

                PutItemACL (setAclRequest);

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
        
        public bool MoveFolder(string folderPath, string destFolderPath, bool throwOnError = false)
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

        public bool MoveFile(string filename,string destFileName, bool throwOnError = false)
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

        public void Dispose()
        {
            if (_client != null)
                _client.Dispose();
            _client = null;
        }
    }
}
