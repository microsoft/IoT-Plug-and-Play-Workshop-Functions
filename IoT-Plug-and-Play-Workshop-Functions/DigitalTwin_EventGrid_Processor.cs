using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.DigitalTwins.Core;
using System.Net.Http;
using Azure.Identity;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Threading.Tasks;
using Azure;
using Azure.Core.Pipeline;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class DigitalTwin_EventGrid_Processor
    {
        private static readonly HttpClient _httpClient = new HttpClient();
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static string _mapKey = Environment.GetEnvironmentVariable("MAP_KEY");
        private static string _mapStatesetId = Environment.GetEnvironmentVariable("StatesetId");
        private static string _mapTemperatureUnitId = Environment.GetEnvironmentVariable("UnitId");
        private static DigitalTwinsClient _adtClient = null;

        [FunctionName("DigitalTwin_EventGrid_Processor")]
        public static async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {

            if (!string.IsNullOrEmpty(_adtServiceUrl))
            {
                try
                {
                    var credentials = new DefaultAzureCredential();
                    _adtClient = new DigitalTwinsClient(new Uri(_adtServiceUrl), credentials, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(_httpClient) });
                    log.LogInformation("ADT service client connection created.");
                }
                catch (Exception e)
                {
                    log.LogError($"ADT service client connection failed. {e}");
                    return;
                }
            }

            if (_adtClient != null)
            {
                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    string twinId = eventGridEvent.Subject.ToString();
                    JObject message = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());

                    log.LogInformation($"Reading event from {twinId}: {eventGridEvent.EventType}: {message["data"]}");

                    // Process Digital Twin Update Event for the room model
                    if (message["data"]["modelId"].ToString() == "dtmi:com:example:Room;1")
                    {
                        if (!string.IsNullOrEmpty(_mapKey) && !string.IsNullOrEmpty(_mapStatesetId) && !string.IsNullOrEmpty(_mapTemperatureUnitId))
                        {
                            string featureId = _mapTemperatureUnitId;

                            foreach (var operation in message["data"]["patch"])
                            {
                                if (operation["op"].ToString() == "replace" && operation["path"].ToString() == "/Temperature")
                                {   //Update the maps feature stateset
                                    var postcontent = new JObject(new JProperty("States", new JArray(
                                        new JObject(new JProperty("keyName", "temperature"),
                                             new JProperty("value", operation["value"].ToString()),
                                             new JProperty("eventTimestamp", DateTime.Now.ToString("s"))))));

                                    log.LogInformation($"********* Updating {featureId} to {operation["value"].ToString()}");

                                    var response = await _httpClient.PostAsync(
                                        $"https://atlas.microsoft.com/featureState/state?api-version=1.0&statesetID={_mapStatesetId}&featureID={featureId}&subscription-key={_mapKey}",
                                        new StringContent(postcontent.ToString()));

                                    log.LogInformation(await response.Content.ReadAsStringAsync());
                                }
                            }
                        }
                    }
                    else
                    {
                        //Find and update parent Twin
                        string parentId = await FindParentAsync(_adtClient, twinId, "contains", log);
                        if (parentId != null)
                        {
                            log.LogInformation($"Parent ID {parentId}");
                            // Read properties which values have been changed in each operation
                            foreach (var operation in message["data"]["patch"])
                            {
                                string opValue = (string)operation["op"];
                                if (opValue.Equals("replace"))
                                {
                                    string propertyPath = ((string)operation["path"]);

                                    if (propertyPath.Equals("/Temperature"))
                                    {
                                        await UpdateTwinPropertyAsync(_adtClient, parentId, propertyPath, operation["value"].Value<float>(), log);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        private static async Task<string> FindParentAsync(DigitalTwinsClient client, string child, string relname, ILogger log)
        {
            // Find parent using incoming relationships
            try
            {
                AsyncPageable<IncomingRelationship> rels = client.GetIncomingRelationshipsAsync(child);

                await foreach (IncomingRelationship ie in rels)
                {
                    if (ie.RelationshipName == relname)
                        return (ie.SourceId);
                }
            }
            catch (RequestFailedException exc)
            {
                log.LogInformation($"*** Error in retrieving parent:{exc.Status}:{exc.Message}");
            }
            return null;
        }

        public static async Task UpdateTwinPropertyAsync(DigitalTwinsClient client, string twinId, string propertyPath, object value, ILogger log)
        {
            // If the twin does not exist, this will log an error
            try
            {
                var updateTwinData = new JsonPatchDocument();
                updateTwinData.AppendReplace(propertyPath, value);

                log.LogInformation($"UpdateTwinPropertyAsync sending {updateTwinData}");
                await client.UpdateDigitalTwinAsync(twinId, updateTwinData);
            }
            catch (RequestFailedException exc)
            {
                log.LogInformation($"*** Error:{exc.Status}/{exc.Message}");
            }
        }
    }
}
