using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
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
        private static ILogger _logger = null;
        private static DeviceModelResolver _resolver = null;

        [FunctionName("Telemetry_Processor")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", ConsumerGroup = "telemetry-functions-cg", Connection = "EVENTHUB_CS")] EventData[] eventData,
                                     [SignalR(HubName = _signalr_Hub)] IAsyncCollector<SignalRMessage> signalRMessage,
                                     ILogger logger)
        {
            var exceptions = new List<Exception>();

            _logger = logger;
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
                            _logger.LogInformation($"IoT Hub Message Source {msgSource}");
                        }

                        _logger.LogInformation($"Telemetry Source  : {msgSource}");
                        _logger.LogInformation($"Telemetry Message : {Encoding.UTF8.GetString(ed.Body.Array, ed.Body.Offset, ed.Body.Count)}");

                        DateTime enqueuTime = (DateTime)ed.SystemProperties["iothub-enqueuedtime"];

                        if (ed.SystemProperties.ContainsKey("dt-dataschema"))
                        {
                            model_id = ed.SystemProperties["dt-dataschema"].ToString();
                        }

                        SIGNALR_DATA signalrData = new SIGNALR_DATA
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
                                await OnTelemetryReceived(signalrData, ed);
                                signalr_target = "DeviceTelemetry";
                                break;
                            case "twinChangeEvents":
                                OnDeviceTwinChanged(signalrData, ed);
                                signalr_target = "DeviceTwinChange";
                                break;
                            case "digitalTwinChangeEvents":
                                OnDigitalTwinTwinChanged(signalrData, ed);
                                signalr_target = "DigitalTwinChange";
                                break;
                            case "deviceLifecycleEvents":
                                OnDeviceLifecycleChanged(signalrData, ed);
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
                        _logger.LogInformation("Unsupported Message Source");
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

        // Process Device Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"OnDeviceTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Digital Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDigitalTwinTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"OnDigitalTwinTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Device Lifecycle Change event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceLifecycleChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"OnDeviceLifecycleChanged");
            signalrData.data = JsonConvert.SerializeObject(eventData.Properties);
        }

        private static async Task<IReadOnlyDictionary<Dtmi, DTEntityInfo>> DeviceModelResolveAndParse(string dtmi)
        {
            if (!string.IsNullOrEmpty(dtmi))
            {
                try
                {
                    if (_resolver == null)
                    {
                        _resolver = new DeviceModelResolver(_modelRepoUrl, _gitToken, _logger);
                    }

                    // resolve and parse device model
                    return await _resolver.ParseModelAsync(dtmi);

                }
                catch (Exception e)
                {
                    _logger.LogError($"Error Resolve(): {e.Message}");
                }
            }

            return null;
        }
        private static void ProcessInterface(SIGNALR_DATA signalrData, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, DTEntityKind entryKind, string keyName, Dtmi keyId, JToken jsonData)
        {
            _logger.LogInformation($"Key ID {keyId} kind {entryKind.ToString()} Value {jsonData}");

            switch (entryKind)
            {
                case DTEntityKind.Field:
                    var entries = parsedModel.Where(r => r.Value.EntityKind == entryKind).Select(x => x.Value as DTFieldInfo).Where(x => x.Id == keyId).ToList();
                    foreach (var entry in entries)
                    {
                        ProcessInterface(signalrData, parsedModel, entry.Schema.EntityKind, entry.Name, entry.Id, jsonData);
                    }
                    break;
                case DTEntityKind.String:
                    JObject signalRData = JObject.Parse(signalrData.data);

                    //signalrData[]
                    break;
                default:
                    _logger.LogInformation($"Unsupported DTEntry Kind {entryKind.ToString()}");
                    break;
            }

        }
        private static void ProcessTelemetryEntry(SIGNALR_DATA signalrData, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, string keyName, JToken jsonData)
        {
            var model = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Telemetry).Select(x => x.Value as DTTelemetryInfo).Where(x => x.Name == keyName).ToList();

            if (model.Count == 1)
            {
                _logger.LogInformation($"Key {keyName} Value {jsonData}");

                switch (model[0].Schema.EntityKind)
                {
                    case DTEntityKind.Object:
                        _logger.LogInformation($"Object");

                        var objectFields = model[0].Schema as DTObjectInfo;
                        foreach (var field in objectFields.Fields)
                        {
                            ProcessInterface(signalrData, parsedModel, DTEntityKind.Field, field.Name, field.Id, jsonData);
                        }

                        break;
                    case DTEntityKind.Enum:
                        _logger.LogInformation($"Enum");
                        var enumEntry = model[0].Schema as DTEnumInfo;
                        JObject signalRData = JObject.Parse(signalrData.data);
                        var value = signalRData[keyName].ToObject<int>();
                        signalRData[keyName] = enumEntry.EnumValues[value].DisplayName["en"];
                        signalrData.data = signalRData.ToString(Formatting.None);

                        break;
                    case DTEntityKind.String:
                    case DTEntityKind.Integer:
                    case DTEntityKind.Float:
                        break;
                    default:
                        _logger.LogInformation($"Unsupported DTEntry King {model[0].Schema.EntityKind.ToString()}");
                        break;
                }
            }
        }

        // Process Telemetry
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        // This function needs refactoring.... to do list..
        private static async Task OnTelemetryReceived(SIGNALR_DATA signalrData, EventData eventData)
        {
            string deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
            string dtmi = string.Empty;
            bool bFoundTwin = false;
            IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel = null;

            _logger.LogInformation($"OnTelemetryReceived");

            //
            // Prepare SignalR data
            //
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

            //
            // Process IoT Plug and Play
            //
            if (eventData.SystemProperties.ContainsKey("dt-dataschema"))
            {
                dtmi = eventData.SystemProperties["dt-dataschema"].ToString();
                parsedModel = await DeviceModelResolveAndParse(dtmi);
            }

            if (parsedModel != null)
            {
                JObject signalRData = JObject.Parse(signalrData.data);

                foreach (KeyValuePair<string, JToken> property in signalRData)
                {
                    ProcessTelemetryEntry(signalrData, parsedModel, property.Key, property.Value);
                }
            }
            else
            {
                return;
            }

            //
            //Process ADT
            //
            // Step 1 : Connect to ADT
            if (string.IsNullOrEmpty(_adtHostUrl))
            {
                return;
            }

            if (_adtClient == null)
            {
                try
                {
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
                    _adtClient = null;
                    _logger.LogError($"Error DigitalTwinsClient(): {e.Message}");
                }
            }

            // Step 2 : check if twin exists for this device
            try
            {
                string query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{deviceId}' AND IS_OF_MODEL('{dtmi}')";
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
                _logger.LogError($"Error QueryAsync(): {e.Message}");
                return;
            }

            // Step 3 : Check if this twin has a parent
            List<BasicDigitalTwin> parentTwins = await FindParentAsync(_adtClient, deviceId, "contains");

            if (parentTwins == null || bFoundTwin == false)
            {
                return;
            }

            // Step 3 : We have twin and Device Model
            var tdList = new List<TELEMETRY_DATA>();
            JObject signalRData = JObject.Parse(signalrData.data);

            List<KeyValuePair<Dtmi, DTEntityInfo>> interfaces = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Telemetry).ToList();

            // we are interested in Temperature and Light 
            // Illuminance 
            // No Semantic for Seeed Wio Terminal

            foreach (var dt in interfaces)
            {
                DTTelemetryInfo telemetryInfo = dt.Value as DTTelemetryInfo;

                var telemetryData = GetTelemetrydata(telemetryInfo, signalRData, dtmi);
                if (telemetryData != null)
                {
                    tdList.Add(telemetryData);
                }
            }

            // Update property of parent (room)
            if (tdList.Count > 0)
            {
                foreach (var telemetry in tdList)
                {
                    _logger.LogInformation($"Telemetry {telemetry.name} data : {telemetry.dataDouble}/{telemetry.dataInteger}");
                    try
                    {
                        var twinPatchData = new JsonPatchDocument();
                        // loop through parents

                        foreach (var parentTwin in parentTwins)
                        {
                            if (parentTwin.Contents.ContainsKey(telemetry.name))
                            {
                                twinPatchData.AppendReplace($"/{telemetry.name}", (telemetry.dataKind == DTEntityKind.Integer) ? telemetry.dataInteger : telemetry.dataDouble);
                            }
                            else
                            {
                                twinPatchData.AppendAdd($"/{telemetry.name}", (telemetry.dataKind == DTEntityKind.Integer) ? telemetry.dataInteger : telemetry.dataDouble);
                            }

                            var updateResponse = await _adtClient.UpdateDigitalTwinAsync(parentTwin.Id, twinPatchData);
                            _logger.LogInformation($"ADT Response : {updateResponse.Status}");
                        }
                    }
                    catch (RequestFailedException e)
                    {
                        _logger.LogError($"Error UpdateDigitalTwinAsync():{e.Status}/{e.ErrorCode} : {e.Message}");
                    }
                }
            }
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
                        if ((supplementalType.Versionless.Equals("dtmi:standard:class:Temperature")))
                        //if ((supplementalType.Versionless.Equals("dtmi:standard:class:Temperature")) ||
                        //(supplementalType.Versionless.Equals("dtmi:standard:class:Illuminance")))
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
        private static async Task<List<BasicDigitalTwin>> FindParentAsync(DigitalTwinsClient client, string child, string relname)
        {
            List<BasicDigitalTwin> twins = new List<BasicDigitalTwin>();
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
                        twins.Add(twin);
                    }
                }
            }
            catch (RequestFailedException e)
            {
                _logger.LogError($"Error FindParentAsync() :{e.Status}:{e.Message}");
            }
            return twins;
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

        public class SIGNALR_DATA
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

    #region ModelResolver
    public class DeviceModelResolver
    {
        private const string _publicRepository = "https://devicemodels.azure.com";
        private static string _privateRepository = string.Empty;
        private static string _githubToken = string.Empty;
        private static HttpClient _httpClient = new HttpClient();
        private static ILogger _logger = null;

        public DeviceModelResolver(ILogger logger)
        {
            _logger = logger;
        }

        public DeviceModelResolver(string repositoryUri)
        {
            _privateRepository = repositoryUri;
        }

        public DeviceModelResolver(string repositoryUri, string githubToken, ILogger logger)
        {
            _privateRepository = repositoryUri;
            _githubToken = githubToken;
            _logger = logger;
        }
        private bool IsValidDtmi(string dtmi)
        {
            // Regex defined at https://github.com/Azure/digital-twin-model-identifier#validation-regular-expressions
            Regex rx = new Regex(@"^dtmi:[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?(?::[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?)*;[1-9][0-9]{0,8}$");
            return rx.IsMatch(dtmi);
        }

        public string DtmiToPath(string dtmi)
        {
            if (IsValidDtmi(dtmi))
            {
                return $"/{dtmi.ToLowerInvariant().Replace(":", "/").Replace(";", "-")}.json";
            }
            return string.Empty;
        }
        public async Task<string> GetModelContentAsync(string dtmiPath)
        {
            return await this.GetModelContentAsync(dtmiPath, false);
        }

        public async Task<string> GetModelContentAsync(string dtmiPath, bool bPublicRepo)
        {
            var jsonModel = string.Empty;
            try
            {
                var fullPath = new Uri($"{(bPublicRepo == true ? _publicRepository : _privateRepository)}{dtmiPath}");
                if (!string.IsNullOrEmpty(_githubToken))
                {
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _githubToken);
                }
                jsonModel = await _httpClient.GetStringAsync(fullPath);
            }
            catch (Exception e)
            {
                _logger.LogError($"Error GetModelContentAsync(): {e.Message}");
            }
            return jsonModel;
        }
        public async Task<IReadOnlyDictionary<Dtmi, DTEntityInfo>> ParseModelAsync(string dtmi)
        {
            string modelContent = string.Empty;
            IReadOnlyDictionary<Dtmi, DTEntityInfo> parseResult = null;
            List<string> listModelJson = new List<string>();

            string dtmiPath = DtmiToPath(dtmi);

            if (!string.IsNullOrEmpty(dtmiPath))
            {
                modelContent = await GetModelContentAsync(dtmiPath, false);

                if (string.IsNullOrEmpty(modelContent))
                {
                    // try public repo
                    modelContent = await GetModelContentAsync(dtmiPath);
                }

                if (!string.IsNullOrEmpty(modelContent))
                {
                    listModelJson.Add(modelContent);
                }

                try
                {
                    ModelParser parser = new ModelParser();
                    //parser.DtmiResolver = (IReadOnlyCollection<Dtmi> dtmis) =>
                    //{
                    //    foreach (Dtmi d in dtmis)
                    //    {
                    //        _logger.LogInformation($"-------------  {d}");
                    //    }
                    //    return null;
                    //};
                    parseResult = await parser.ParseAsync(listModelJson);
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error ParseModelAsync(): {e.Message}");
                }
            }
            return parseResult;
        }
    }
    #endregion // resolver
}
