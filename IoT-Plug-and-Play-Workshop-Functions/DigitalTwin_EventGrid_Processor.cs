using System;
using Microsoft.Azure.WebJobs;
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
using System.Net;
using System.Collections.Generic;
using Microsoft.Azure.EventGrid.Models;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class DigitalTwin_EventGrid_Processor
    {
        private static readonly HttpClient _httpClient = new HttpClient();
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static string _mapKey = Environment.GetEnvironmentVariable("MAP_KEY");
        private static string _mapStatesetId = Environment.GetEnvironmentVariable("MAP_STATESET_ID");
        private static string _mapDatasetId = Environment.GetEnvironmentVariable("MAP_DATASET_ID");
        private static DigitalTwinsClient _adtClient = null;
        private static List<MapUnit> UnitList = new List<MapUnit>();

        [FunctionName("DigitalTwin_EventGrid_Processor")]
        public static async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            if (_adtClient == null && !string.IsNullOrEmpty(_adtServiceUrl))
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

            if (_adtClient != null && !string.IsNullOrEmpty(_mapKey) && !string.IsNullOrEmpty(_mapStatesetId))
            {
                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    string twinId = eventGridEvent.Subject.ToString();
                    JObject message = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                    string unitId = string.Empty;

                    log.LogInformation($"Received {eventGridEvent.EventType} from {twinId} : {message["data"]}");

                    // Process Digital Twin Update Event for the room model
                    if ((message["data"]["modelId"].ToString().StartsWith("dtmi:com:example:Room")))
                    {
                        // Find Unit ID from cached list
                        MapUnit unit = UnitList.Find(x => x.twinId == twinId);

                        if (unit == null)
                        {
                            try
                            {
                                // Get Digital Twin for the room being updated
                                Response<BasicDigitalTwin> roomTwin = await _adtClient.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);

                                if (roomTwin == null)
                                {
                                    // this should not happen
                                    log.LogError($"Digital Twin for {twinId} not found");
                                    return;
                                }

                                log.LogInformation($"Found Room Twin {roomTwin.Value.Id}");

                                // Check if unitid is already set or not
                                if (roomTwin.Value.Contents.ContainsKey("unitId"))
                                {
                                    unitId = roomTwin.Value.Contents["unitId"].ToString();
                                    log.LogInformation($"Found Unit ID {unitId}");
                                }
                                else
                                {
                                    // Unit ID not set.
                                    string roomNumber = string.Empty;
                                    log.LogInformation($"Unit ID not found for {twinId}");

                                    // See if this is "adding" a room number
                                    foreach (var operation in message["data"]["patch"])
                                    {
                                        if (operation["op"].ToString() == "add" && operation["path"].ToString() == "/roomNumber")
                                        {
                                            roomNumber = operation["value"].ToString();
                                            break;
                                        }
                                    }

                                    if (string.IsNullOrEmpty(roomNumber))
                                    {
                                        if (roomTwin.Value.Contents.ContainsKey("roomNumber"))
                                        {
                                            roomNumber = roomTwin.Value.Contents["roomNumber"].ToString();
                                        }
                                    }

                                    if (!string.IsNullOrEmpty(roomNumber))
                                    {
                                        log.LogInformation($"Getting Unit ID from Azure Map for {roomNumber}");
                                        unitId = await getUnitId(roomNumber, log);
                                        log.LogInformation($"Got Unit ID from Azure Map {unitId}");

                                        if (!string.IsNullOrEmpty(unitId))
                                        {
                                            log.LogInformation("Caching Unit ID data");
                                            // Cache unit ID
                                            unit = new MapUnit();
                                            unit.twinId = twinId;
                                            unit.unitId = unitId;
                                            UnitList.Add(unit);

                                            // Update Room Twin so we don't have to query Azure Map.
                                            await UpdateTwinPropertyAsync(_adtClient, twinId, "/unitId", unitId, false, log);
                                        }
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                log.LogError($"Error Searching Digital Twin {twinId} failed : {e.Message}");
                                return;
                            }
                        }
                        else
                        {
                            unitId = unit.unitId;
                            log.LogInformation($"Unit ID {unitId} found in cache");
                        }

                        if (!string.IsNullOrEmpty(unitId))
                        {
                            string featureId = unitId;

                            log.LogInformation($"Message Data : {message["data"]}");

                            foreach (var operation in message["data"]["patch"])
                            {
                                if ((operation["path"].ToString() == "/temperature") ||
                                    (operation["path"].ToString() == "/light") ||
                                    (operation["path"].ToString() == "/co2")
                                    )
                                {
                                    string opValue = operation["op"].ToString();
                                    log.LogInformation($"Found {operation["path"].ToString().Replace("/", "")} : {operation["op"].ToString()}");

                                    if (opValue.Equals("replace") || opValue.Equals("add"))
                                    {   //Update the maps feature stateset
                                        var postcontent = new JObject(new JProperty("States", new JArray(
                                            new JObject(new JProperty("keyName", operation["path"].ToString().Replace("/", "")),
                                                 new JProperty("value", operation["value"].ToString()),
                                                 new JProperty("eventTimestamp", DateTime.Now.ToString("s"))))));

                                        log.LogInformation($"Updating Map Unit v2 {featureId} {operation["path"].ToString()} to {operation["value"].ToString()}");

                                        var response = await _httpClient.PutAsync(
                                            $"https://us.atlas.microsoft.com/featureStateSets/{_mapStatesetId}/featureStates/{featureId}?api-version=2.0&subscription-key={_mapKey}",
                                            new StringContent(postcontent.ToString()));

                                        log.LogInformation(await response.Content.ReadAsStringAsync());
                                    }
                                }
                                else if (operation["path"].ToString() == "/occupied")
                                {
                                    HttpResponseMessage response = null;
                                    string opValue = operation["op"].ToString();

                                    if (opValue.Equals("replace") || opValue.Equals("add"))
                                    {   //Update the maps feature stateset

                                        //if (operation["value"].ToString().ToLower().Equals("false"))
                                        //{
                                        //    response = await _httpClient.DeleteAsync(
                                        //        $"https://us.atlas.microsoft.com/featureStateSets/{_mapStatesetId}/featureStates/{featureId}?api-version=2.0&subscription-key={_mapKey}&stateKeyName=occupied"
                                        //        );
                                        //}
                                        //else
                                        {
                                            var postcontent = new JObject(new JProperty("States", new JArray(
                                                new JObject(new JProperty("keyName", operation["path"].ToString().Replace("/", "")),
                                                     new JProperty("value", operation["value"].ToString()),
                                                     new JProperty("eventTimestamp", DateTime.Now.ToString("s"))))));

                                            log.LogInformation($"Updating Map Unit v2 {featureId} {operation["path"].ToString()} to {operation["value"].ToString()}");

                                            response = await _httpClient.PutAsync(
                                                $"https://us.atlas.microsoft.com/featureStateSets/{_mapStatesetId}/featureStates/{featureId}?api-version=2.0&subscription-key={_mapKey}",
                                                new StringContent(postcontent.ToString()));

                                        }

                                        log.LogInformation(await response.Content.ReadAsStringAsync());

                                    }


                                }
                            }
                        }
                    }
                }
            }
        }

        public static async Task UpdateTwinPropertyAsync(DigitalTwinsClient client, string twinId, string propertyPath, object value, bool bPatch, ILogger log)
        {
            // If the twin does not exist, this will log an error
            try
            {
                var updateTwinData = new JsonPatchDocument();

                if (bPatch)
                {
                    updateTwinData.AppendReplace(propertyPath, value);
                }
                else
                {
                    updateTwinData.AppendAdd(propertyPath, value);
                }

                log.LogInformation($"UpdateTwinPropertyAsync sending {updateTwinData}");
                await client.UpdateDigitalTwinAsync(twinId, updateTwinData);
            }
            catch (RequestFailedException e)
            {
                log.LogError($"Error UpdateTwinPropertyAsync():{e.Status}/{e.ErrorCode} : {e.Message}");
            }
        }

        private static async Task<string> getUnitId(string roomNumber, ILogger log)
        {
            //https://github.com/Azure-Samples/LiveMaps/tree/main/src

            string unitId = string.Empty;

            string url = $"https://us.atlas.microsoft.com/wfs/datasets/{_mapDatasetId}/collections/unit/items?api-version=2.0&limit=1&subscription-key={_mapKey}&name={roomNumber}";

            using (var client = new HttpClient())
            {
                log.LogInformation($"Sending GET to Map {url}");
                HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Get, url);
                var response = await client.SendAsync(requestMessage);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    var result = await response.Content.ReadAsStringAsync();

                    log.LogInformation($"Response {result}");
                    var features = JsonConvert.DeserializeObject<FeatureCollection>(result);

                    if (features.NumberReturned == 1)
                    {
                        log.LogInformation($"Found a feature {features.Features[0].Id} name {features.Features[0].Properties.Name}");
                        unitId = features.Features[0].Id;
                    }
                }
                else
                {
                    log.LogError($"Query feature failed {response.StatusCode}");
                }
            }

            return unitId;
        }

        public class MapUnit
        {
            public string twinId { get; set; }
            public string unitId { get; set; }
        }
        public class FeatureCollection
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("features")]
            public Feature[] Features { get; set; }

            [JsonProperty("numberReturned")]
            public long NumberReturned { get; set; }

            public link[] links { get; set; }
        }

        public class link
        {
            public string href { get; set; }
            public string rel { get; set; }
        }

        public partial class Properties
        {
            [JsonProperty("originalId")]
            public Guid OriginalId { get; set; }

            [JsonProperty("categoryId")]
            public string CategoryId { get; set; }

            [JsonProperty("isOpenArea")]
            public bool IsOpenArea { get; set; }

            [JsonProperty("isRoutable")]
            public bool isRoutable { get; set; }

            [JsonProperty("routeThroughBehavior")]
            public string RouteThroughBehavior { get; set; }

            [JsonProperty("levelId")]
            public string LevelId { get; set; }

            [JsonProperty("occupants")]
            public object[] Occupants { get; set; }

            [JsonProperty("addressId")]
            public string AddressId { get; set; }

            [JsonProperty("name")]
            public string Name { get; set; }
        }
        public partial class Geometry
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("coordinates")]
            public double[][][] Coordinates { get; set; }
        }
        public partial class Feature
        {
            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("geometry")]
            public Geometry Geometry { get; set; }

            [JsonProperty("properties")]
            public Properties Properties { get; set; }

            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("featureType")]
            public string featureType { get; set; }
        }
    }
}
