using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Azure;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.DigitalTwins.Parser;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IoT_Plug_and_Play_Workshop_Functions
{
    public static class Telemetry_Processor
    {
        private const string _signalr_Hub = "telemetryhub";
        private static readonly string _adtHostUrl = Environment.GetEnvironmentVariable("ADT_HOST_URL");
        private static readonly string _modelRepoUrl = Environment.GetEnvironmentVariable("ModelRepository");
        private static readonly string _gitToken = Environment.GetEnvironmentVariable("GitToken");
        private static readonly HttpClient _httpClient = new HttpClient();
        private static readonly WebClient _webClient = new WebClient();
        private static DigitalTwinsClient _adtClient = null;

        [FunctionName("Telemetry_Processor")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", ConsumerGroup = "telemetry-functions-cg", Connection = "EVENTHUB_CS")] EventData[] eventData,
                                     [SignalR(HubName = _signalr_Hub)] IAsyncCollector<SignalRMessage> signalRMessage,
                                     ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData ed in eventData)
            {
                try
                {
                    if (ed.SystemProperties.ContainsKey("iothub-message-source"))
                    {
                        string deviceId = ed.SystemProperties["iothub-connection-device-id"].ToString();
                        string msgSource = ed.SystemProperties["iothub-message-source"].ToString();
                        string signalr_target = string.Empty;
                        string model_id = string.Empty;

                        if (msgSource != "Telemetry")
                        {
                            log.LogInformation($"IoT Hub Message Source {msgSource}");
                        }

                        log.LogInformation($"Telemetry Source  : {msgSource}");
                        log.LogInformation($"Telemetry Message : {Encoding.UTF8.GetString(ed.Body.Array, ed.Body.Offset, ed.Body.Count)}");

                        DateTime enqueuTime = (DateTime)ed.SystemProperties["iothub-enqueuedtime"];

                        if (ed.SystemProperties.ContainsKey("dt-dataschema"))
                        {
                            model_id = ed.SystemProperties["dt-dataschema"].ToString();
                        }

                        NOTIFICATION_DATA signalrData = new NOTIFICATION_DATA
                        {
                            eventId = ed.SystemProperties["x-opt-sequence-number"].ToString(),
                            eventType = "Event Hubs",
                            eventTime = enqueuTime.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                            eventSource = msgSource,
                            deviceId = deviceId,
                            dtDataSchema = model_id,
                            data = null
                        };

                        // Process telemetry based on message source
                        switch (msgSource)
                        {
                            case "Telemetry":
                                await OnTelemetryReceived(signalrData, ed, log);
                                signalr_target = "DeviceTelemetry";
                                break;
                            case "twinChangeEvents":
                                OnDeviceTwinChanged(signalrData, ed, log);
                                signalr_target = "DeviceTwinChange";
                                break;
                            case "digitalTwinChangeEvents":
                                OnDigitalTwinTwinChanged(signalrData, ed, log);
                                signalr_target = "DigitalTwinChange";
                                break;
                            case "deviceLifecycleEvents":
                                OnDeviceLifecycleChanged(signalrData, ed, log);
                                signalr_target = "DeviceLifecycle";
                                break;
                            default:
                                break;
                        }

                        if (signalrData.data != null)
                        {
                            // send to SignalR Hub
                            var data = JsonConvert.SerializeObject(signalrData);

                            await signalRMessage.AddAsync(new SignalRMessage
                            {
                                Target = signalr_target,
                                Arguments = new[] { data }
                            });
                        }

                        signalrData = null;
                    }
                    else
                    {
                        log.LogInformation("Unsupported Message Source");
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        // Process Telemetry
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        // This function needs refactoring.... to do list..
        private static async Task OnTelemetryReceived(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            string deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
            string model_id = string.Empty;
            var tdList = new List<TELEMETRY_DATA>();
            bool bFoundTwin = false;

            log.LogInformation($"OnTelemetryReceived");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

            if (string.IsNullOrEmpty(_adtHostUrl))
            {
                return;
            }

            if (!eventData.SystemProperties.ContainsKey("dt-dataschema"))
            {
                return;
            }

            model_id = eventData.SystemProperties["dt-dataschema"].ToString();

            if (string.IsNullOrEmpty(model_id))
            {
                return;
            }

            if (_adtClient == null)
            {
                try
                {
                    //AsyncPageable<ModelData> modelList;
                    //ManagedIdentityCredential cred2 = new ManagedIdentityCredential("https://digitaltwins.azure.net");
                    //DigitalTwinsClientOptions opts = new DigitalTwinsClientOptions { Transport = new HttpClientTransport(httpClient) };
                    //DigitalTwinsClient client2 = new DigitalTwinsClient(new Uri(adtHostUrl), cred2, opts);
                    //modelList = client2.GetModelsAsync(null, true);

                    //Authenticate with Digital Twins
                    var credential = new DefaultAzureCredential();
                    ManagedIdentityCredential cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");
                    _adtClient = new DigitalTwinsClient(new Uri(_adtHostUrl),
                                                        credential,
                                                        new DigitalTwinsClientOptions
                                                        {
                                                            Transport = new HttpClientTransport(_httpClient)
                                                        });
                }
                catch (Exception e)
                {
                    log.LogError($"Error DigitalTwinsClient(): {e.Message}");
                    return;
                }
            }

            try
            {
                string query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{deviceId}' AND IS_OF_MODEL('{model_id}')";
                // Make sure digital twin node exist for this device
                AsyncPageable<BasicDigitalTwin> asyncPageableResponse = _adtClient.QueryAsync<BasicDigitalTwin>(query);
                await foreach (BasicDigitalTwin twin in asyncPageableResponse)
                {
                    if (twin.Id == deviceId)
                    {
                        bFoundTwin = true;
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError($"Error QueryAsync(): {e.Message}");
                return;
            }

            if (bFoundTwin)
            {
                BasicDigitalTwin parentTwin = await FindParentAsync(_adtClient, deviceId, "contains", log);

                if (parentTwin == null)
                {
                    // no relationship.  Nothing to do
                    return;
                }

                // Resolve Device Model
                var dtmiContent = string.Empty;
                try
                {
                    dtmiContent = await Resolve(model_id);
                }
                catch (Exception e)
                {
                    log.LogError($"Error Resolve(): {e.Message}");
                    return;
                }

                if (!string.IsNullOrEmpty(dtmiContent))
                {
                    ModelParser parser = new ModelParser();
                    parser.DtmiResolver = DtmiResolver;
                    var parsedDtmis = await parser.ParseAsync(new List<string> { dtmiContent });

                    var interfaces = parsedDtmis.Where(r => r.Value.EntityKind == DTEntityKind.Telemetry).ToList();

                    // we are interested in Temperature and Light 
                    // Illuminance 
                    // No Semantic for Seeed Wio Terminal
                    foreach (var dt in interfaces)
                    {
                        DTTelemetryInfo telemetryInfo = dt.Value as DTTelemetryInfo;
                        JObject signalRData = JObject.Parse(signalrData.data);
                        var telemetryData = GetTelemetrydata(telemetryInfo, signalRData, model_id);
                        if (telemetryData != null)
                        {
                            tdList.Add(telemetryData);
                        }
                    }
                }

                // Update property of parent (room)

                if (tdList.Count > 0)
                {
                    foreach(var telemetry in tdList)
                    {
                        log.LogInformation($"Telemetry {telemetry.name} data : {telemetry.dataDouble}/{telemetry.dataInteger}");
                        try
                        {
                            var twinPatchData = new JsonPatchDocument();
                            if (parentTwin.Contents.ContainsKey(telemetry.name))
                            {
                                twinPatchData.AppendReplace($"/{telemetry.name}", (telemetry.dataKind == DTEntityKind.Integer) ? telemetry.dataInteger : telemetry.dataDouble);
                            }
                            else
                            {
                                twinPatchData.AppendAdd($"/{telemetry.name}", (telemetry.dataKind == DTEntityKind.Integer) ? telemetry.dataInteger : telemetry.dataDouble);
                            }

                            var updateResponse = await _adtClient.UpdateDigitalTwinAsync(parentTwin.Id, twinPatchData);
                            log.LogInformation($"ADT Response : {updateResponse.Status}");
                        }
                        catch (RequestFailedException e)
                        {
                            log.LogError($"Error UpdateDigitalTwinAsync():{e.Status}/{e.ErrorCode} : {e.Message}");
                        }
                    }
                }
            }
        }

        // Process Device Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceTwinChanged(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnDeviceTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Digital Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDigitalTwinTwinChanged(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnDigitalTwinTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Device Lifecycle Change event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceLifecycleChanged(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnDeviceLifecycleChanged");
            signalrData.data = JsonConvert.SerializeObject(eventData.Properties);
        }

        private static TELEMETRY_DATA GetTelemetrydata(DTTelemetryInfo telemetryInfo, JObject signalRData, string model_id)
        {
            TELEMETRY_DATA data = null;
            bool bFoundData = false;
            string semanticType = string.Empty;

            if ((telemetryInfo.Schema.EntityKind == DTEntityKind.Integer) || (telemetryInfo.Schema.EntityKind == DTEntityKind.Double))
            {
                if ((telemetryInfo.SupplementalTypes.Count == 0) && model_id.StartsWith("dtmi:seeedkk:wioterminal:wioterminal_aziot_example"))
                {
                    if (telemetryInfo.Name.Equals("light"))
                    {
                        semanticType = "dtmi:standard:class:Illuminance";
                        bFoundData = true;
                    }
                }
                else if ((telemetryInfo.SupplementalTypes.Count == 0) && model_id.StartsWith("dtmi:seeedkk:wioterminal:wioterminal_co2checker"))
                {
                    if (telemetryInfo.Name.Equals("co2"))
                    {
                        semanticType = "";
                        bFoundData = true;
                    }
                }
                else
                {
                    foreach (var supplementalType in telemetryInfo.SupplementalTypes)
                    {
                        if ((supplementalType.Versionless.Equals("dtmi:standard:class:Temperature")) ||
                            (supplementalType.Versionless.Equals("dtmi:standard:class:Illuminance")))
                        {
                            bFoundData = true;
                            semanticType = supplementalType.Versionless;
                            break;
                        }
                    }
                }

                if (bFoundData)
                {
                    data = new TELEMETRY_DATA();
                    data.dataKind = telemetryInfo.Schema.EntityKind;
                    data.name = telemetryInfo.Name;
                    if (data.dataKind == DTEntityKind.Integer)
                    {
                        data.dataInteger = (long)signalRData[telemetryInfo.Name];
                    }
                    else
                    {
                        data.dataDouble = (double)signalRData[telemetryInfo.Name];
                    }
                    data.dataName = telemetryInfo.Name;
                    data.semanticType = semanticType;
                }
            }

            return data;
        }
        private static async Task<BasicDigitalTwin> FindParentAsync(DigitalTwinsClient client, string child, string relname, ILogger log)
        {
            // Find parent using incoming relationships
            Response<BasicDigitalTwin> twin = null;
            try
            {
                string sourceId = string.Empty;
                AsyncPageable<IncomingRelationship> rels = client.GetIncomingRelationshipsAsync(child);

                await foreach (IncomingRelationship ie in rels)
                {
                    if (ie.RelationshipName.Equals(relname))
                    {
                        twin = await client.GetDigitalTwinAsync<BasicDigitalTwin>(ie.SourceId);
                        break;
                    }
                }
            }
            catch (RequestFailedException e)
            {
                log.LogError($"Error FindParentAsync() :{e.Status}:{e.Message}");
            }
            return (twin != null) ? twin.Value : null;
        }

        private static async Task<string> Resolve(string dtmi)
        {
            if (string.IsNullOrEmpty(dtmi))
            {
                return string.Empty;
            }

            // Apply model repository convention
            string dtmiPath = DtmiToPath(dtmi.ToString());

            if (string.IsNullOrEmpty(dtmiPath))
            {
                //log.LogWarning($"Invalid DTMI: {dtmi}");
                return await Task.FromResult<string>(string.Empty);
            }

            string modelContent = string.Empty;

            // if private repo is provided, resolve model with private repo first.
            if (!string.IsNullOrEmpty(_modelRepoUrl))
            {
                modelContent = getModelContent(_modelRepoUrl, dtmiPath, _gitToken);
            }

            if (string.IsNullOrEmpty(modelContent))
            {
                modelContent = getModelContent("https://devicemodels.azure.com", dtmiPath, string.Empty);
            }

            return modelContent;
        }

        public static async Task<IEnumerable<string>> DtmiResolver(IReadOnlyCollection<Dtmi> dtmis)
        {
            List<String> jsonLds = new List<string>();

            foreach (var dtmi in dtmis)
            {
                Console.WriteLine("Resolver looking for. " + dtmi);
                string model = dtmi.OriginalString.Replace(":", "/");
                model = (model.Replace(";", "-")).ToLower();
                if (!String.IsNullOrWhiteSpace(model))
                {
                    var dtmiContent = await Resolve(dtmi.OriginalString);
                    jsonLds.Add(dtmiContent);
                }
            }
            return jsonLds;
        }

        private static string getModelContent(string repoUrl, string dtmiPath, string gitToken)
        {
            string modelContent = string.Empty;
            Uri modelRepoUrl = new Uri(repoUrl);
            Uri fullPath = new Uri($"{modelRepoUrl}{dtmiPath}");
            string fullyQualifiedPath = fullPath.ToString();

            if (!string.IsNullOrEmpty(gitToken))
            {
                var token = $"token {gitToken}";
                _webClient.Headers.Add("Authorization", token);
            }

            try
            {
                modelContent = _webClient.DownloadString(fullyQualifiedPath);
            }
            catch (System.Net.WebException e)
            {
                Console.WriteLine($"Exception in getModelContent() : {e.Message}");
                return string.Empty;
            }

            return modelContent;
        }

        private static bool IsValidDtmi(string dtmi)
        {
            // Regex defined at https://github.com/Azure/digital-twin-model-identifier#validation-regular-expressions
            Regex rx = new Regex(@"^dtmi:[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?(?::[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?)*;[1-9][0-9]{0,8}$");
            return rx.IsMatch(dtmi);
        }

        private static string DtmiToPath(string dtmi)
        {
            if (!IsValidDtmi(dtmi))
            {
                return null;
            }
            // dtmi:com:example:Thermostat;1 -> dtmi/com/example/thermostat-1.json
            return $"/{dtmi.ToLowerInvariant().Replace(":", "/").Replace(";", "-")}.json";
        }

        public class TELEMETRY_DATA
        {
            public string dataName { get; set; }
            public string semanticType { get; set; }
            public DTEntityKind dataKind { get; set; }
            public double dataDouble { get; set; }
            public double dataInteger { get; set; }
            public string name { get; set; }
        }

        public class NOTIFICATION_DATA
        {
            public string eventId { get; set; }
            public string eventType { get; set; }
            public string deviceId { get; set; }
            public string eventSource { get; set; }
            public string eventTime { get; set; }
            public string data { get; set; }
            public string dtDataSchema { get; set; }
        }
    }
}
