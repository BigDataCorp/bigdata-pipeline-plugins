using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProcessRunnerPlugin
{
    public class Task : IActionModule
    {
        string _lastError;
        Record _options;
        ActionLogger _logger;
        Process _task;
        bool fireAndForget;

        public string Name { get; private set; }

        public string Description { get; private set; }

        public Task ()
        {
            Name = "Task";
            Description = "Task";
        }

        public IEnumerable<PluginParameterDetails> GetParameterDetails ()
        {
            yield return new PluginParameterDetails ("processFilename", typeof (string), "Filename of the executable file (.exe, .bat etc) to be called", true);
            yield return new PluginParameterDetails ("parameters", typeof (string), "Process parameters in command line style (separated by spaces and may be surrounded by quotation marks)", false);
            yield return new PluginParameterDetails ("waitInSeconds", typeof (string), "Wait timeout in seconds for the process finalization. If none or 0 than it will wait until the process finish.", false);
            yield return new PluginParameterDetails ("processWorkingDirectory", typeof (string), "Working directory for the process. If none is providade, the default value is the same directory of the executing file", false);
            yield return new PluginParameterDetails ("fireAndForget", typeof (bool), "If the process should be started and left alone aftwards", false);
        }

        public void CleanUp ()
        {
            
        }

        public string GetLastError ()
        {
            return _lastError;
        }
        
        public void SetParameters (Record options, ActionLogger logger)
        {
            _options = options;
            _logger = logger;
        }

        public bool Execute (params IEnumerable<Record>[] dataStreams)
        {
            _lastError = null;

            try
            {
                var processFilename = _options.Get ("processFilename", "");
                if (String.IsNullOrWhiteSpace (processFilename))
                    throw new ArgumentNullException ("processFilename");

                var taskWorkDir = _options.Get ("processWorkingDirectory", "");
                if (String.IsNullOrWhiteSpace (taskWorkDir))
                {
                    taskWorkDir = System.IO.Path.GetDirectoryName (processFilename);
                }

                fireAndForget = _options.Get ("fireAndForget", false);

                return ExecuteProcess (processFilename,
                    _options.Get ("parameters", ""),
                    TimeSpan.FromSeconds (_options.Get ("waitInSeconds", 0)),
                    !fireAndForget,
                    !fireAndForget,
                    taskWorkDir);
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        public bool ExecuteProcess (string processName, string parameters = "")
        {
            try
            {
                _task = Process.Start (new ProcessStartInfo (processName, parameters));
                _task.WaitForExit ();
                if (_task.ExitCode != 0)
                    throw new Exception (String.Format ("QueriesUtils.ExecuteProcess [ExitCode = {0}] [processName = {1}] [parameters = {2}]", _task.ExitCode, processName, parameters));
            }
            catch (Exception ex)
            {
                _logger.Error (ex);
                return false;
            }
            finally
            {
                Cancel ();
            }
            return true;
        }

        public bool ExecuteProcess (string processName, string parameters, out string result)
        {
            result = String.Empty;
            try
            {
                _task = new System.Diagnostics.Process ();
                _task.StartInfo.FileName = processName;
                _task.StartInfo.Arguments = parameters;
                _task.StartInfo.UseShellExecute = false;
                _task.StartInfo.CreateNoWindow = true;
                _task.StartInfo.RedirectStandardOutput = true;
                //proc.StartInfo.RedirectStandardError = true;
                if (!_task.Start ())
                    throw new Exception ("QueriesUtils.ExecuteProcess [process start failed]");
                _task.WaitForExit ();
                //proc.StandardError.ReadToEnd ();
                result = _task.StandardOutput.ReadToEnd ();
                return _task.HasExited && _task.ExitCode == 0;
            }
            catch (Exception ex)
            {
                result = ex.Message;
                return false;
            }
            finally
            {
                Cancel ();
            }
        }

        public bool ExecuteProcess (string processName, string parameters, TimeSpan? waitTimeout, bool standardOutput, bool errorOutput, string taskWorkDir)
        {
            // clean up before execution
            Cancel ();

            // execution
            try
            {
                _task = new Process ();

                if (!String.IsNullOrWhiteSpace (taskWorkDir))
                    _task.StartInfo.WorkingDirectory = taskWorkDir;
                if (!String.IsNullOrWhiteSpace (parameters))
                    _task.StartInfo.Arguments = parameters;
                _task.StartInfo.FileName = processName;
                _task.StartInfo.UseShellExecute = false;
                _task.StartInfo.CreateNoWindow = true;

                // check fire and forget option
                if (fireAndForget)
                {
                    standardOutput = false;
                    errorOutput = false;
                }

                // configure output streams
                if (standardOutput)
                {
                    _task.StartInfo.RedirectStandardOutput = true;
                    _task.OutputDataReceived += progressInfo;
                }
                if (errorOutput)
                {
                    _task.StartInfo.RedirectStandardError = true;
                    _task.ErrorDataReceived += progressError;
                }

                // Start task
                _task.Start ();

                // begin listening to output streams
                if (errorOutput)
                    _task.BeginOutputReadLine ();
                if (standardOutput)
                    _task.BeginErrorReadLine ();

                // wait for process finish
                if (fireAndForget)
                {
                    return true;
                }
                else if (waitTimeout.HasValue && waitTimeout.Value.TotalMilliseconds > 0)
                {
                    int ms = (int)waitTimeout.Value.TotalMilliseconds;
                    while (!_task.WaitForExit(ms))
                    {
                        ms -= (int)(DateTime.UtcNow - _task.StartTime.ToUniversalTime ()).TotalMilliseconds;
                        if (ms < 0)
                            break;
                    }
                }
                else
                {
                    _task.WaitForExit ();
                }

                // make sure process has ended
                // http://msdn.microsoft.com/en-us/library/ty0d8k56.aspx
                _task.WaitForExit (100);

                // log last lines
                if (infoHead.Count > 0)
                    _logger.Log ("Output:\n" + String.Join ("\n", infoHead) + "\n(...)\n" + String.Join ("\n", infoTail));

                // check exit code
                try
                {
                    if (!_task.HasExited)
                        _logger.Warn ("ExitCode: process didn't exit before timeout");
                    else if (_task.ExitCode < 0)
                        _logger.Error ("ExitCode: " + _task.ExitCode);
                    else
                        _logger.Log ("ExitCode: " + _task.ExitCode);
                } 
                catch
                {
                    _logger.Warn ("ExitCode: process didn't exit before timeout");
                }

                // return exit code
                return _task.HasExited && _task.ExitCode == 0;
            }           
            finally
            {
                // clean up after execution
                Cancel ();
            }
        }

        private void Cancel ()
        {
            try
            {
                if (_task != null && !fireAndForget)
                {
                    using (var proc = _task)
                    {
                        _task = null;                        
                        proc.ErrorDataReceived -= progressInfo;
                        proc.OutputDataReceived -= progressInfo;
                        proc.Refresh ();
                        if (!proc.HasExited)
                        {
                            try { proc.CloseMainWindow (); }
                            catch { }
                        }
                        if (!proc.HasExited)
                            proc.WaitForExit (100);
                        proc.Refresh ();
                        if (!proc.HasExited)
                            proc.Kill ();
                        proc.Close ();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error ("Error trying to stop/close process", ex);
            }
            _task = null;
        }


        List<string> infoHead = new List<string> (3);
        List<string> infoTail = new List<string> (5);

        void progressInfo (object sender, DataReceivedEventArgs e)
        {
            if (e == null)
                return;
            string strMessage = e.Data;
            if (!String.IsNullOrWhiteSpace (strMessage))
            {
                if (!_logger.IsTracingEnabled)
                {
                    if (infoHead.Count < 3)
                    {
                        infoHead.Add (DateTime.UtcNow.ToString ("HH:mm:ss") + "\t" + strMessage);   
                    }
                    else
                    {
                        if (infoTail.Count > 4)
                            infoTail.RemoveAt (0);
                        infoTail.Add (DateTime.UtcNow.ToString ("HH:mm:ss") + "\t" + strMessage);
                    }
                }
                else
                { 
                    _logger.Debug (strMessage);
                }
            }
        }
                
        void progressError (object sender, DataReceivedEventArgs e)
        {
            if (e == null)
                return;
            string strMessage = e.Data;
            if (!String.IsNullOrWhiteSpace (strMessage))
            {
                _logger.Error (strMessage);
            }
        }
    }
}
