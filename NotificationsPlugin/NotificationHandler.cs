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
        public string GetDescription ()
        {
            return "Plugin used to send emails, SMS or to make phone calls";
        } 

        /// <summary>
        /// Gets the Descriptions of the parameters that this plugin
        /// will use
        /// </summary>
        /// <returns>List of Plugin Parameters Details</returns>
        public IEnumerable<ModuleParameterDetails> GetParameterDetails ()
        {
           yield return new ModuleParameterDetails ("notificationType", typeof (string), "Possible Types: CALL, SMS, EMAIL. Defaults to EMAIL.", true);
           yield return new ModuleParameterDetails ("contacts", typeof (string), "List of contacts (emails / phones) separated by spaces, comma", true);
           yield return new ModuleParameterDetails ("subject", typeof (string), "Subject (used in case of email)", false);
           yield return new ModuleParameterDetails ("message", typeof (string), "Message to be sent on the notification", true); 
   
           // Email Specific Parameters
           yield return new ModuleParameterDetails ("emailFrom", typeof (string), "Address from which the email will be sent", false); 
           yield return new ModuleParameterDetails ("emailLogin", typeof (string), "Login of the email from which the message will be sent", false); 
           yield return new ModuleParameterDetails ("emailPassword", typeof (string), "Email dredential password", false); 
           yield return new ModuleParameterDetails ("emailSmtp", typeof (string), "email smtp server address", false); 
           yield return new ModuleParameterDetails ("emailPort", typeof (int), "Port of the smtp server", false);
           yield return new ModuleParameterDetails ("emailSsl", typeof(bool), "True if ssl is enabled, false otherwise", false);
           yield return new ModuleParameterDetails ("emailIsHtml", typeof(bool), "True if the email contains HTML, false otherwise. Defaults to true.", false); 
        }

        /// <summary>
        /// Implements this plugin execution's logic
        /// </summary>
        public bool Execute (ISessionContext context)
        {
            var logger = context.GetLogger ();
            var options = context.Options;

            try
            {
                // Parsing Parameters
                String notificationType = options.Get ("notificationType", "EMAIL");
                String contacts         = options.Get ("contacts",         String.Empty);   
                String subject          = options.Get ("subject",          String.Empty);   
                String message          = options.Get ("message",          String.Empty);

                // Email Specific Configurations
                String EmailFrom     = options.Get ("emailFrom",     String.Empty);
                String EmailLogin    = options.Get ("emailLogin",    String.Empty);
                String EmailPassword = options.Get ("emailPassword", String.Empty);
                String EmailSmtp     = options.Get ("emailSmtp",     "");
                int    EmailPort     = options.Get ("emailPort",     587); // Gmail port is the default
                bool   EmailSsl      = options.Get ("emailSsl",      false);
                bool   EmailIsHtml   = options.Get ("emailIsHtml",   true);

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

                switch (notificationType.ToUpperInvariant())
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
                List<String> contactsList = contacts.Split (' ', ',', ';', '|').Where (i => !String.IsNullOrEmpty(i)).ToList();

                // Sending Notification
                var notificationResponse = NotificationService.Send (notifType, contactsList, message, subject);

                // Checking for error
                if (!notificationResponse.Status)
                {
                    logger.Error("Error Sending Email:" + notificationResponse.Message);
                }

                return notificationResponse.Status;
            }
            catch (Exception ex)
            {
                context.Error = ex.Message;
                logger.Error (ex);
                return false;
            }
        }
    }
}
