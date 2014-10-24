using MongoDB.Bson;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MongoDbActionsPlugin
{
    public class MongoObjectIdConverter : JsonConverter
    {
        /// <summary>
        /// The JsonSerializerSettings configured to uses this converter for the ObjectId type
        /// </summary>
        public static JsonSerializerSettings DefaultSettings { get; set; }

        static MongoObjectIdConverter ()
        {
            DefaultSettings = new JsonSerializerSettings
            {
                Converters = new[] { new MongoObjectIdConverter () },
                //DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                Formatting = Formatting.None,
                MissingMemberHandling = MissingMemberHandling.Ignore,
                NullValueHandling = NullValueHandling.Ignore,
                ObjectCreationHandling = ObjectCreationHandling.Replace
            };
        }

        /// <summary>
        /// Add this converter to the default settings of Newtonsoft JSON.NET.
        /// </summary>
        public static void UseAsDefaultJsonConverter ()
        {
            Newtonsoft.Json.JsonConvert.DefaultSettings = () => DefaultSettings;
        }

        public override void WriteJson (JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize (writer, value.ToString ());
        }

        public override object ReadJson (JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            ObjectId obj;
            string value = serializer.Deserialize<string> (reader);
            ObjectId.TryParse (value, out obj);
            return obj;
        }

        public override bool CanConvert (Type objectType)
        {
            return typeof (ObjectId).IsAssignableFrom (objectType);
        }
    }
}
