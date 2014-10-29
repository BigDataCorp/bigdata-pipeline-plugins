using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NotificationsPlugin
{
    public class NotificationHandler : IActionModule
    {
        #region ** Private Attributes **

        private string       _lastError; // Holds the Last Error Message
        private ActionLogger _logger;    // 
        private Record       _options;   // Parameters passed to this Plugin
        ISessionContext _context;

        #endregion

        #region ** Public Attributes **

        public string Description { get; private set; }

        public string Name        { get; private set; }

        #endregion

        /// <summary>
        /// Class Constructor
        /// </summary>
        public NotificationHandler ()
        {
            Name        = "Notifications Plugin ";
            Description = "Plugin used to send emails, SMS or to make phone calls";
        }

        /// <summary>
        /// Gets the Descriptions of the parameters that this plugin
        /// will use
        /// </summary>
        /// <returns>List of Plugin Parameters Details</returns>
        public IEnumerable<PluginParameterDetails> GetParameterDetails ()
        {
           yield return new PluginParameterDetails ("NotificationType", typeof (string), "Possible Types: CALL, SMS, EMAIL", true);
           yield return new PluginParameterDetails ("Contacts", typeof (string), "List of contacts (emails / phones) separated by spaces", true);
           yield return new PluginParameterDetails ("Subject", typeof (string), "Subject (used in case of email)", false);
           yield return new PluginParameterDetails ("Message", typeof (string), "Message to be sent on the notification", true); 
   
           // Email Specific Parameters
           yield return new PluginParameterDetails ("EmailFrom", typeof (string), "Address from which the email will be sent", false); 
           yield return new PluginParameterDetails ("EmailLogin", typeof (string), "Login of the email from which the message will be sent", false); 
           yield return new PluginParameterDetails ("EmailPassword", typeof (string), "Email dredential password", false); 
           yield return new PluginParameterDetails ("EmailSmtp", typeof (string), "email smtp server address", false); 
           yield return new PluginParameterDetails ("EmailPort", typeof (int), "Port of the smtp server", false);
           yield return new PluginParameterDetails ("EmailSsl", typeof(bool), "True if ssl is enabled, false otherwise", false);
           yield return new PluginParameterDetails ("EmailIsHtml", typeof(bool), "True if the email contains HTML, false otherwise", false); 
        }

        /// <summary>
        /// Sets the Parameters and Logger for this plugin
        /// </summary>
        /// <param name="options">Plugin Parameters</param>
        /// <param name="context">The context.</param>
        public void SetParameters (Record options, ISessionContext context)
        {
            _context = context;
            _options = options;
            _logger = _context.GetLogger ();
        }

        /// <summary>
        /// Implements this plugin execution's logic
        /// </summary>
        /// <param name="dataStreams"></param>
        /// <returns></returns>
        public bool Execute (params IEnumerable<Record>[] dataStreams)
        {
            _lastError = null;

            try
            {
                // Parsing Parameters
                String notificationType = _options.Get ("NotificationType", "EMAIL");
                String contacts         = _options.Get ("Contacts",         String.Empty);   
                String subject          = _options.Get ("Subject",          String.Empty);   
                String message          = _options.Get ("Message",          String.Empty);

                // Email Specific Configurations
                String EmailFrom     = _options.Get ("EmailFrom",     String.Empty);
                String EmailLogin    = _options.Get ("EmailLogin",    String.Empty);
                String EmailPassword = _options.Get ("EmailPassword", String.Empty);
                String EmailSmtp     = _options.Get ("EmailSmtp",     "");
                int    EmailPort     = _options.Get ("EmailPort",     587); // Gmail port is the default
                bool   EmailSsl      = _options.Get ("EmailSsl",      false);
                bool   EmailIsHtml   = _options.Get ("EmailIsHtml",   false);

                // Setting up Configuration
                NotificationService.Configuration = new NotificationConfig ()
                {
                    EmailFrom = EmailFrom,
                    EmailLogin = EmailLogin,
                    EmailPassword = EmailPassword,
                    EmailSmtp = EmailSmtp,
                    EmailPort = EmailPort,
                    EmailSsl  = EmailSsl,
                    EmailIsHtml = EmailIsHtml
                };

                // Picking up Notification Type
                NotificationTypes notifType = NotificationTypes.EMAIL;

                switch (notificationType.ToUpper())
                {
                    case "SMS":
                        notifType = NotificationTypes.SMS;
                    break;

                    case "CALL":
                        notifType = NotificationTypes.CALL;
                    break;

                    case "EMAIL":
                        notifType = NotificationTypes.EMAIL;
                    break;
                }

                // Parsing Contacts List
                List<String> contactsList = contacts.Split (' ').ToList();

                // Sending Notification
                var notificationResponse = NotificationService.Send (notifType, contactsList, message, subject);

                // Checking for error
                if (!notificationResponse.Status)
                {
                    _logger.Error("Error Sending Email:" + notificationResponse.Message);
                }

                return notificationResponse.Status;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _logger.Error ("[ex] " + _lastError);
                return false;
            }
        }

        /// <summary>
        /// Gets the Last Error Message
        /// </summary>
        /// <returns></returns>
        public string GetLastError ()
        {
            return _lastError;
        }

        /// <summary>
        /// CleanUp Method for this plugin, that should
        /// reset any needed attribute / instance
        /// </summary>
        public void CleanUp ()
        {
            _lastError = String.Empty;
        }
    }
}
