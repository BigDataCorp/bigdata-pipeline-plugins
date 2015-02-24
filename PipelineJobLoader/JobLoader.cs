using BigDataPipeline;
using BigDataPipeline.Core.Interfaces;
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
        IStorageModule _storageContext;

        public string GetDescription ()
        {
            return "Load jobs from a text file and updates the system jobs database";
        }

        public IEnumerable<PluginParameterDetails> GetParameterDetails ()
        {
            yield return new PluginParameterDetails ("JobLoader.SearchPath", typeof (string), "Input Search path with wildcards", true);
            yield return new PluginParameterDetails ("JobLoader.BackupPath", typeof (string), "Path to the directory to where the loaded files will be copied to");
            yield return new PluginParameterDetails ("JobLoader.awsAccessKey", typeof (string), "AWS Access key for S3 usage");
            yield return new PluginParameterDetails ("JobLoader.awsSecretAccessKey", typeof (string), "AWS Secret Access key for S3 usage");
        }

        public PipelineJob GetJobRegistrationDetails ()
        {            
            var c = new PipelineJob
            {
                Id = this.GetType ().FullName,
                Name = this.GetType ().Name,
                Domain = "System",
                Enabled = true,
                Scheduler = new List<string> { "*/30 * * * *" },
                NextExecution = DateTime.UtcNow,
                RootAction = new ActionDetails
                {
                    Module = this.GetType ().FullName,
                    Type = ModuleTypes.SystemModule
                }
            };
            return c;
        }

        public void SetSystemParameters (IStorageModule storageContext)
        {
            _storageContext = storageContext;
        }

        public bool Execute (ISessionContext context)
        {
            var logger = context.GetLogger ();
            var options = context.Options;            

            try
            {
                var inputPath = options.Get ("JobLoader.SearchPath", System.IO.Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "Jobloader", "Input", "*"));
                var backupPath = options.Get ("JobLoader.BackupPath", System.IO.Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "Jobloader", "Backup"));

                if (String.IsNullOrEmpty (inputPath))
                    throw new ArgumentNullException ("JobLoader.SearchPath");
                var searchOptions = System.IO.SearchOption.AllDirectories;
                var dir = System.IO.Path.GetDirectoryName (inputPath);
                var searchPattern = System.IO.Path.GetFileName (inputPath);
                foreach (var f in System.IO.Directory.EnumerateFiles (dir, searchPattern, searchOptions).ToList ())
                {
                    logger.Info ("File found: " + f);
                    // load and parse file content
                    var encoding = SimpleHelpers.FileEncoding.DetectFileEncoding (f);
                    if (encoding == null)
                    {
                        logger.Debug ("Ignoring file: could not detect file encoding");  
                        continue;
                    }
                    var json = System.IO.File.ReadAllText (f, encoding);
                    var item = Newtonsoft.Json.JsonConvert.DeserializeObject<PipelineJob> (json);

                    // save to database
                    if (item != null)
                    {
                        item.SetScheduler (item.Scheduler != null ? item.Scheduler.ToArray () : new string[0]);
                        _storageContext.SavePipelineCollection (item);
                        logger.Info ("Pipeline imported: " + item.Id);
                    }
                    
                    // move file
                    if (!String.IsNullOrEmpty (backupPath))
                    {
                        (new System.IO.DirectoryInfo (backupPath)).Create ();
                        System.IO.File.Move (f, System.IO.Path.Combine (backupPath, System.IO.Path.GetFileName (f)));
                        logger.Debug ("File moved to backup folder");
                    }
                }
            }
            catch (Exception ex)
            {
                context.Error = ex.Message;
                logger.Error (ex);
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
