using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace NotificationsPlugin
{
    public class CommonUtils
    {
        static readonly char[] ASCII_LOOKUP_TABLE_ACCENT =
        {
            'À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ',
            'Ò', 'Ó', 'Ô', 'Õ', 'Ö', '×', 'Ø', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'Þ', 'ß', 'à', 'á', 'â',
            'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í', 'î', 'ï', 'ð', 'ñ', 'ò', 'ó',
            'ô', 'õ', 'ö', '÷', 'ø', 'ù', 'ú', 'û'
        };
        
        static readonly char[] ASCII_LOOKUP_TABLE_ACCENT_FREE =
        {
            'A', 'A', 'A', 'A', 'A', 'A', 'A', 'C', 'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I', 'D', 'N',
            'O', 'O', 'O', 'O', 'O', 'X', '0', 'U', 'U', 'U', 'U', 'Y', 'Þ', 's', 'a', 'a', 'a',
            'a', 'a', 'a', 'a', 'c', 'e', 'e', 'e', 'e', 'i', 'i', 'i', 'i', 'o', 'n', 'o', 'o',
            'o', 'o', 'o', '÷', '0', 'u', 'u', 'u'
        };

        static readonly int ASCIILookupTableMinPos = ASCII_LOOKUP_TABLE_ACCENT[0];
        static readonly int ASCIILookupTableMaxPos = ASCII_LOOKUP_TABLE_ACCENT[ASCII_LOOKUP_TABLE_ACCENT.Length - 1];


        private static Regex regexEmail = null;
        static public bool ValidadeEmail (string email)
        {
            if (String.IsNullOrEmpty (email))
                return false;
            // check for regex lazy initialization
            if (regexEmail == null)
            {
                regexEmail = new Regex (@"^([0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*@([0-9a-zA-Z][-\w]*[0-9a-zA-Z]\.)+[a-zA-Z]{2,9})$", RegexOptions.Compiled);
            }
            return regexEmail.Match (email).Success;
        }

        public static string ParseNumericString (string txt, int maxLength = 0)
        {
            return txt.Where (t => Char.IsNumber (t)).ToString ();
        }

        /// <summary>
        /// Removes the accentuation to upper.
        /// </summary>
        /// <param name="newTxt">The new TXT.</param>
        /// <returns></returns>

        public static IEnumerable<char> RemoveAccentuation (IEnumerable<char> txt)
        {
            foreach (var c in txt)
            {
                // update if in valid value range
                yield return ((c >= ASCIILookupTableMinPos) && (c <= ASCIILookupTableMaxPos)) ? ASCII_LOOKUP_TABLE_ACCENT_FREE[c - ASCIILookupTableMinPos] : c;
            }
        }
    }
}
