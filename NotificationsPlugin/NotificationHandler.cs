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
        /// Implements this plugin execution's logic
        /// </summary>
        public bool Execute (ISessionContext context)
        {
            var logger = context.GetLogger ();
            var options = context.Options;

            try
            {
                // Parsing Parameters
                String notificationType = options.Get ("NotificationType", "EMAIL");
                String contacts         = options.Get ("Contacts",         String.Empty);   
                String subject          = options.Get ("Subject",          String.Empty);   
                String message          = options.Get ("Message",          String.Empty);

                // Email Specific Configurations
                String EmailFrom     = options.Get ("EmailFrom",     String.Empty);
                String EmailLogin    = options.Get ("EmailLogin",    String.Empty);
                String EmailPassword = options.Get ("EmailPassword", String.Empty);
                String EmailSmtp     = options.Get ("EmailSmtp",     "");
                int    EmailPort     = options.Get ("EmailPort",     587); // Gmail port is the default
                bool   EmailSsl      = options.Get ("EmailSsl",      false);
                bool   EmailIsHtml   = options.Get ("EmailIsHtml",   false);

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
