using BigDataPipeline;
using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipelineJobLoader
{
    public class JobLoader : ISystemModule
    {
        string _lastError;
        string _inputPath;
        string _backupPath;
        IStorageContext _storageContext;
        ActionLogger _logger;
        ISessionContext _context;

        public string Name { get; private set; }

        public string Description { get; private set; }

        public JobLoader ()
        {
            Name = "JobLoader";
            Description = "Load jobs from a text file and updates the system jobs database";
        }

        public IEnumerable<PluginParameterDetails> GetParameterDetails ()
        {
            yield return new PluginParameterDetails ("JobLoader.SearchPath", typeof (string), "Input Search path with wildcards", true);
            yield return new PluginParameterDetails ("JobLoader.BackupPath", typeof (string), "Path to the directory to where the loaded files will be copied to");
            yield return new PluginParameterDetails ("JobLoader.awsAccessKey", typeof (string), "AWS Access key for S3 usage");
            yield return new PluginParameterDetails ("JobLoader.awsSecretAccessKey", typeof (string), "AWS Secret Access key for S3 usage");
        }

        public void SetSystemParameters (IStorageContext storageContext)
        {
            _storageContext = storageContext;
        }

        public void SetParameters (Record options, ISessionContext context)
        {
            _context = context;
            _logger = _context.GetLogger ();
            _inputPath = options.Get ("JobLoader.SearchPath", System.IO.Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "Jobloader", "Input", "*"));
            _backupPath = options.Get ("JobLoader.BackupPath", System.IO.Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "Jobloader", "Backup"));
        }        

        public string GetLastError ()
        {
            return _lastError;
        }

        public void CleanUp ()
        {            
        }

        public PipelineCollection GetJobDetails ()
        {
            var c= new PipelineCollection
            {
                Id = this.GetType ().FullName,
                Name = this.GetType ().Name,
                Domain = "System",
                Enabled = true,
                Jobs = new List<PipelineJob>
                {
                    new PipelineJob ()
                    {                        
                        Enabled = true,
                        Scheduler = new List<string> {"*/30 * * * *"},
                        NextExecution = DateTime.UtcNow,
                        RootAction = new ActionDetails
                        {
                            PluginId = this.GetType ().FullName,
                            Type = PluginTypes.SystemModule
                        }
                    }
                }
                
            };
            return c;
        }

        public bool Execute (params IEnumerable<Record>[] dataStreams)
        {
            try
            {
                if (String.IsNullOrEmpty (_inputPath))
                    throw new ArgumentNullException ("JobLoader.SearchPath");
                var searchOptions = System.IO.SearchOption.AllDirectories;
                var dir = System.IO.Path.GetDirectoryName (_inputPath);
                var searchPattern = System.IO.Path.GetFileName (_inputPath);
                foreach (var f in System.IO.Directory.EnumerateFiles (dir, searchPattern, searchOptions).ToList ())
                {
                    _logger.Log ("File found: " + f);
                    // load and parse file content
                    var encoding = SimpleHelpers.FileEncoding.DetectFileEncoding (f);
                    if (encoding == null)
                    {
                        _logger.Log ("Ignoring file: could not detect file encoding");  
                        continue;
                    }
                    var json = System.IO.File.ReadAllText (f, Encoding.GetEncoding (encoding));
                    var item = Newtonsoft.Json.JsonConvert.DeserializeObject<PipelineCollection> (json);

                    // save to database
                    if (item != null)
                    {
                        foreach (var i in item.Jobs)
                            i.SetScheduler (i.Scheduler != null ? i.Scheduler.ToArray () : new string[0]);
                        _storageContext.SavePipelineCollection (item);
                        _logger.Log ("Pipeline imported: " + item.Id);
                    }
                    
                    // move file
                    if (!String.IsNullOrEmpty (_backupPath))
                    {
                        (new System.IO.DirectoryInfo (_backupPath)).Create ();
                        System.IO.File.Move (f, System.IO.Path.Combine (_backupPath, System.IO.Path.GetFileName (f)));
                        _logger.Log ("File moved to backup folder");
                    }
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;                
            }

            return true;
        }

        public static string WildcardToRegex (string pattern)
        {
            // (new System.Text.RegularExpressions.Regex (pattern,RegexOptions.IgnoreCase)).IsMatch (txt)

            return "^" + System.Text.RegularExpressions.Regex.Escape (pattern).
                               Replace (@"\*", ".*").
                               Replace (@"\?", ".") + "$";            
        }
    }
}
