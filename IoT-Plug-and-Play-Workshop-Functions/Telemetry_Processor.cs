using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
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
        private static readonly string _modelRepoUrl = Environment.GetEnvironmentVariable("PRIVATE_MODEL_REPOSIROTY_URL");
        private static readonly string _gitToken = Environment.GetEnvironmentVariable("PRIVATE_MODEL_REPOSIROTY_TOKEN");
        private static string _mapKey = Environment.GetEnvironmentVariable("MAP_KEY");
        private static string _mapStatesetId = Environment.GetEnvironmentVariable("MAP_STATESET_ID");
        private static string _mapDatasetId = Environment.GetEnvironmentVariable("MAP_DATASET_ID");
        private static readonly HttpClient _httpClient = new HttpClient();
        private static DigitalTwinsClient _adtClient = null;
        private static ILogger _logger = null;
        private static DeviceModelResolver _resolver = null;
        private static List<PHONE_POSE_DATA> phonePoseList = new List<PHONE_POSE_DATA>();

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
                        // look for device id
                        string deviceId = ed.SystemProperties["iothub-connection-device-id"].ToString();
                        string msgSource = ed.SystemProperties["iothub-message-source"].ToString();
                        string signalr_target = string.Empty;
                        string model_id = string.Empty;

                        if (msgSource != "Telemetry")
                        {
                            _logger.LogInformation($"IoT Hub Message Source {msgSource}");
                        }

                        _logger.LogInformation($"Telemetry Message : {Encoding.UTF8.GetString(ed.Body.Array, ed.Body.Offset, ed.Body.Count)}");

                        DateTime enqueuTime = (DateTime)ed.SystemProperties["iothub-enqueuedtime"];

                        // look for IoT Plug and Play model id (DTMI)
                        if (ed.SystemProperties.ContainsKey("dt-dataschema"))
                        {
                            model_id = ed.SystemProperties["dt-dataschema"].ToString();
                        }

                        // Initialize SignalR Data
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
                                await OnDeviceTwinChanged(signalrData, ed);
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

        private static async Task<BasicDigitalTwin> FindDigitalTwin(string dtId, string dtmi)
        {
            BasicDigitalTwin twin = null;

            //
            //Process ADT
            //
            if (!string.IsNullOrEmpty(_adtHostUrl))
            {
                // Step 1 : Connect to ADT
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
                    string query;

                    if (string.IsNullOrEmpty(dtmi))
                    {
                        query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{dtId}'";
                    }
                    else
                    {
                        query = $"SELECT * FROM DigitalTwins T WHERE $dtId = '{dtId}' AND IS_OF_MODEL('{dtmi}')";
                    }

                    // Make sure digital twin node exist for this device
                    AsyncPageable<BasicDigitalTwin> asyncPageableResponse = _adtClient.QueryAsync<BasicDigitalTwin>(query);

                    await foreach (BasicDigitalTwin dt in asyncPageableResponse)
                    {
                        if (dt.Id == dtId)
                        {
                            twin = dt;
                            break;
                        }
                    }

                    if (twin == null)
                    {
                        _logger.LogWarning($"Did not find DT {dtId}");
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error ADT QueryAsync(): {e.Message}");
                }
            }

            return twin;
        }

        private static string GetRoomIdInPayload(SIGNALR_DATA signalRData)
        {
            JObject jsonData = JObject.Parse(signalRData.data);
            string roomId = string.Empty;

            if (jsonData.ContainsKey("properties"))
            {
                JObject twinProperties = (JObject)jsonData["properties"];

                if (twinProperties.ContainsKey("reported"))
                {
                    JObject twinReported = (JObject)twinProperties["reported"];

                    if (twinReported.ContainsKey("readOnlyProp"))
                    {
                        _logger.LogInformation($"Found PaaD");

                        roomId = twinReported["readOnlyProp"].ToString().Trim();
                    }
                }
            }

            return roomId;
        }

        private static async Task<bool> SetRoomOccupiedValue(BasicDigitalTwin roomTwin, bool occupied)
        {
            var propertyName = "occupied";
            var twinPatchData = new JsonPatchDocument();

            if (roomTwin.Contents.ContainsKey(propertyName))
            {
                twinPatchData.AppendReplace($"/{propertyName}", occupied);
            }
            else
            {
                twinPatchData.AppendAdd($"/{propertyName}", occupied);
            }

            var response = await _adtClient.UpdateDigitalTwinAsync(roomTwin.Id, twinPatchData);
            _logger.LogInformation($"Updated Digital Twin 'occupied for {roomTwin.Id} to {occupied}");

            return ((response.Status < 200 || response.Status > 299)) ? false : true;
        }

        private static async Task<bool> RemoveRoom2DeviceRelationship(string deviceId)
        {
            AsyncPageable<IncomingRelationship> incomingRelationships = _adtClient.GetIncomingRelationshipsAsync(deviceId);

            await foreach (IncomingRelationship incomingRelationship in incomingRelationships)
            {
                _logger.LogInformation($"Found an incoming relationship '{incomingRelationship.RelationshipId}' from '{incomingRelationship.SourceId}'.");

                // Reset map state
                // Get Feature ID (Unit)

                var featureId = string.Empty;

                // Make sure digital twin node exist for this device
                Response<BasicDigitalTwin> roomTwin = await _adtClient.GetDigitalTwinAsync<BasicDigitalTwin>(incomingRelationship.SourceId);

                if (roomTwin == null)
                {
                    // this should not happen
                    _logger.LogError($"Digital Twin for {incomingRelationship.SourceId} not found");
                    return false;
                }

                _logger.LogInformation($"Found Room Twin {roomTwin.Value.Id}");

                // Check if unitid is already set or not
                if (roomTwin.Value.Contents.ContainsKey("unitId"))
                {
                    featureId = roomTwin.Value.Contents["unitId"].ToString();
                    _logger.LogInformation($"Found Unit ID {featureId}");

                    var response = await _httpClient.DeleteAsync(
                        $"https://us.atlas.microsoft.com/featureStateSets/{_mapStatesetId}/featureStates/{featureId}?api-version=2.0&subscription-key={_mapKey}&stateKeyName=occupied"
                        );
                }

                //if (response.StatusCode < 200 || response.StatusCode > 299)
                //{
                //    return false;
                //}

                var adtResponse = await _adtClient.DeleteRelationshipAsync(incomingRelationship.SourceId, incomingRelationship.RelationshipId);

                if (adtResponse.Status < 200 || adtResponse.Status > 299)
                {
                    return false;
                }
            }

            return true;
        }

        private static async Task<bool> CreateRoom2DeviceRelationship(string roomId, BasicDigitalTwin deviceTwin)
        {
            bool bRet = false;

            var roomTwin = await FindDigitalTwin(roomId, "dtmi:com:example:Room;2");

            //roomTwin = await FindDigitalTwin(roomId, string.Empty);

            if (roomTwin != null)
            {
                // Creating a relationship
                var buildingFloorRelationshipPayload = new BasicRelationship
                {
                    Id = $"Room_{deviceTwin.Id}",
                    SourceId = roomTwin.Id,
                    TargetId = deviceTwin.Id,
                    Name = "contains",
                };

                Response<BasicRelationship> response = await _adtClient
                    .CreateOrReplaceRelationshipAsync<BasicRelationship>(roomTwin.Id, $"Room_{deviceTwin.Id}", buildingFloorRelationshipPayload);

                if (response.GetRawResponse().Status == 200)
                {
                    // Relationship created.
                    // Set occupied to tru
                    // bRet = await SetRoomOccupiedValue(roomTwin, true);
                }
            }

            return bRet;
        }
        // Process Device Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static async Task OnDeviceTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            string deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
            BasicDigitalTwin dtDevice = null;
            _logger.LogInformation($"OnDeviceTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

            dtDevice = await FindDigitalTwin(deviceId, string.Empty);

            if (dtDevice != null)
            {
                //Found Digital Twin for this device
                if (dtDevice.Metadata.ModelId.StartsWith("dtmi:azureiot:PhoneAsADevice"))
                {
                    List<BasicDigitalTwin> parentTwins = await FindParentAsync(_adtClient, deviceId, "contains");
                    var roomId = GetRoomIdInPayload(signalrData);

                    if (string.IsNullOrEmpty(roomId))
                    {
                        // nothing to do
                    }
                    else if (parentTwins.Count > 1)
                    {
                        // Multiple parents.
                        // Not supported
                        _logger.LogWarning($"Found {parentTwins.Count} parents for {deviceId}.  Not supported");

                    }
                    else if (roomId.Equals("0") && parentTwins.Count == 0)
                    {
                        // Room Id = 0 (Remove) but no parent.
                        // Nothing to do
                    }
                    else if (roomId.Equals("0") && parentTwins.Count == 1)
                    {
                        // Has a room parent and user specified room number 0
                        // 1. Remove the exisiting relationship
                        // 2. Set 'occupied' property to false

                        var bRet = await RemoveRoom2DeviceRelationship(deviceId);

                        if (bRet == false)
                        {
                            _logger.LogError($"Failed to remove relationship between {roomId} and {deviceId}");
                        }
                        else
                        {
                            bRet = await SetRoomOccupiedValue(parentTwins[0], false);
                        }
                    }
                    else if (!roomId.Equals("0") && parentTwins.Count == 0)
                    {
                        // Do not have a room parent and user specified room number 0
                        // 1. Create a new relationship
                        // 2. Set 'occupied' property to true

                        var bRet = await CreateRoom2DeviceRelationship(roomId, dtDevice);

                        if (bRet == false)
                        {
                            _logger.LogError($"Failed to create a relationship between {roomId} and {deviceId}");
                        }
                    }
                    else if (!roomId.Equals("0") && parentTwins.Count == 1)
                    {
                        // There is an existing relationship
                        // 1. Check if the relationship exists or not.
                        // 2. If the existing relationship does not match to the specified room, remove existing one and create one.

                        bool bRet = false;

                        if (!roomId.Equals(parentTwins[0].Id))
                        {
                            // A different room.  Remove existing relation
                            bRet = await RemoveRoom2DeviceRelationship(deviceId);

                            if (bRet == false)
                            {
                                _logger.LogError($"Failed to remove relationship between {roomId} and {deviceId}");
                            }

                            bRet = await CreateRoom2DeviceRelationship(roomId, dtDevice);
                        }
                    }
                }
            }
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

        // Process Telemetry
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        // This function needs refactoring.... to do list..
        private static async Task OnTelemetryReceived(SIGNALR_DATA signalrData, EventData eventData)
        {
            string deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
            string componentName = string.Empty;
            string dtmi = string.Empty;
            BasicDigitalTwin digitalTwin = null;
            IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel = null;

            _logger.LogInformation($"OnTelemetryReceived");

            if (eventData.SystemProperties.ContainsKey("dt-subject"))
            {
                componentName = eventData.SystemProperties["dt-subject"].ToString();
            }
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

                if (parsedModel != null)
                {
                    JObject jobjSignalR = JObject.Parse(signalrData.data);

                    foreach (KeyValuePair<string, JToken> property in jobjSignalR)
                    {
                        ProcessTelemetryEntry(signalrData, parsedModel, property.Key, property.Value);
                    }
                }

                digitalTwin = await FindDigitalTwin(deviceId, dtmi);

                // Step 3 : Check if this twin has a parent
                List<BasicDigitalTwin> parentTwins = await FindParentAsync(_adtClient, deviceId, "contains");

                if (parentTwins.Count == 0 || digitalTwin == null)
                {
                    return;
                }

                // Step 3 : We have twin and Device Model
                var tdList = new List<TELEMETRY_DATA>();
                JObject signalRData = JObject.Parse(signalrData.data);

                //List<KeyValuePair<Dtmi, DTEntityInfo>> dtComponentList = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Component).ToList();

                List<KeyValuePair<Dtmi, DTEntityInfo>> dtTelemetryList = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Telemetry).ToList();

                // we are interested in Temperature and Light 
                // Illuminance 
                // No Semantic for Seeed Wio Terminal

                foreach (var dtTelemetry in dtTelemetryList)
                {
                    DTTelemetryInfo dtTelemetryInfo = dtTelemetry.Value as DTTelemetryInfo;

                    var telemetryData = GetTelemetrydataForDigitalTwinProperty(dtTelemetryInfo, signalRData, dtmi);
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
                        var propertyName = string.Empty;

                        if (telemetry.telmetryInterfaceId.Contains("dtmi:atmark_techno:Armadillo:EnvSensor") && telemetry.name.Equals("e_co2"))
                        {
                            propertyName = "co2";
                        }
                        else
                        {
                            propertyName = telemetry.name;
                        }

                        try
                        {
                            var twinPatchData = new JsonPatchDocument();

                            foreach (var parentTwin in parentTwins)
                            {
                                // loop through parents
                                if (telemetry.telmetryInterfaceId.StartsWith("dtmi:azureiot:PhoneSensors"))
                                {
                                    PHONE_POSE_DATA phoneDataListItem = phonePoseList.Find(x => x.deviceId == deviceId);

                                    var upDown = (telemetry.dataDouble > 0) ? PHONE_POSE_UP_DOWN.Up : PHONE_POSE_UP_DOWN.Down;

                                    if (phoneDataListItem == null)
                                    {
                                        phoneDataListItem = new PHONE_POSE_DATA();
                                        phoneDataListItem.deviceId = deviceId;
                                        phoneDataListItem.pose = PHONE_POSE_UP_DOWN.Unkonwn;
                                        phonePoseList.Add(phoneDataListItem);

                                    }

                                    if (phoneDataListItem.pose != upDown)
                                    {
                                        // Update Digital Twin
                                        await SetRoomOccupiedValue(parentTwin, upDown == PHONE_POSE_UP_DOWN.Up ? true : false);
                                    }

                                    phoneDataListItem.pose = upDown;
                                }
                                else
                                {
                                    if (parentTwin.Contents.ContainsKey(propertyName))
                                    {
                                        twinPatchData.AppendReplace($"/{propertyName}", (telemetry.dataKind == DTEntityKind.Integer) ? telemetry.dataInteger : telemetry.dataDouble);
                                    }
                                    else
                                    {
                                        twinPatchData.AppendAdd($"/{propertyName}", (telemetry.dataKind == DTEntityKind.Integer) ? telemetry.dataInteger : telemetry.dataDouble);
                                    }
                                }

                                var updateResponse = await _adtClient.UpdateDigitalTwinAsync(parentTwin.Id, twinPatchData);
                                _logger.LogInformation($"ADT Response : {updateResponse.Status}");

                                //if (string.IsNullOrEmpty(componentName))
                                //{
                                //    var updateResponse = await _adtClient.UpdateDigitalTwinAsync(parentTwin.Id, twinPatchData);
                                //    _logger.LogInformation($"ADT Response : {updateResponse.Status}");
                                //}
                                //else
                                //{
                                //    var updateResponse = await _adtClient.UpdateComponentAsync(parentTwin.Id, componentName, twinPatchData);
                                //    _logger.LogInformation($"ADT Response : {updateResponse.Status}");
                                //}
                            }
                        }
                        catch (RequestFailedException e)
                        {
                            _logger.LogError($"Error UpdateDigitalTwinAsync():{e.Status}/{e.ErrorCode} : {e.Message}");
                        }
                    }
                }
            }
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
           // _logger.LogInformation($"Key ID {keyId} kind {entryKind.ToString()} Value {jsonData}");

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
                case DTEntityKind.Integer:
                case DTEntityKind.Float:
                case DTEntityKind.Double:
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
                //_logger.LogInformation($"Key {keyName} Value {jsonData}");

                switch (model[0].Schema.EntityKind)
                {
                    case DTEntityKind.Object:
                        //_logger.LogInformation($"Object");

                        var objectFields = model[0].Schema as DTObjectInfo;
                        foreach (var field in objectFields.Fields)
                        {
                            ProcessInterface(signalrData, parsedModel, DTEntityKind.Field, field.Name, field.Id, jsonData);
                        }
                        break;

                    case DTEntityKind.Enum:
                        //_logger.LogInformation($"Enum");
                        var enumEntry = model[0].Schema as DTEnumInfo;
                        JObject signalRData = JObject.Parse(signalrData.data);
                        var value = signalRData[keyName].ToObject<int>();
                        if (enumEntry.EnumValues.Count < value)
                        {
                            signalRData[keyName] = enumEntry.EnumValues[value].DisplayName["en"];
                            signalrData.data = signalRData.ToString(Formatting.None);
                        }
                        break;

                    case DTEntityKind.Double:
                    case DTEntityKind.String:
                    case DTEntityKind.Float:
                        break;

                    case DTEntityKind.Integer:
                        if (model[0].Name == "co2" || model[0].Name == "e_co2") // For Seeed Wio Terminal & AtMark Armadillo
                        {
                            break;
                        }
                        break;
                    default:
                        _logger.LogInformation($"Unsupported DTEntry King {model[0].Schema.EntityKind.ToString()}");
                        break;
                }
            }
        }

        private static TELEMETRY_DATA GetTelemetrydataForDigitalTwinProperty(DTTelemetryInfo telemetryInfo, JObject signalRData, string model_id)
        {
            TELEMETRY_DATA data = null;
            bool bFoundTelemetry = false;
            string semanticType = string.Empty;

            if ((telemetryInfo.Schema.EntityKind == DTEntityKind.Integer) || (telemetryInfo.Schema.EntityKind == DTEntityKind.Double) || (telemetryInfo.Schema.EntityKind == DTEntityKind.Float))
            {
                if (telemetryInfo.SupplementalTypes.Count == 0)
                {
                    // No semantics
                    // Look for 
                    // Wio Terminal : light
                    // Wio Terminal : co2
                    // Armadillo : co2
                    // PaaD : battery

                    if (model_id.StartsWith("dtmi:seeedkk:wioterminal:wioterminal_aziot_example"))
                    {
                        if (telemetryInfo.Name.Equals("light"))
                        {
                            semanticType = "dtmi:standard:class:Illuminance";
                            bFoundTelemetry = true;
                        }
                    }
                    else if (model_id.StartsWith("dtmi:seeedkk:wioterminal:wioterminal_co2checker"))
                    {
                        if (telemetryInfo.Name.Equals("co2"))
                        {
                            semanticType = "";
                            bFoundTelemetry = true;
                        }
                    }
                    else if (model_id.StartsWith("dtmi:atmark_techno:Armadillo:IoT_GW_A6_EnvMonitor;1"))
                    {
                        if (telemetryInfo.Name.Equals("e_co2"))
                        {
                            semanticType = "";
                            bFoundTelemetry = true;
                        }
                    }
                    else if (model_id.StartsWith("dtmi:azureiot:PhoneAsADevice"))
                    {
                        if (telemetryInfo.Name.Equals("battery"))
                        {
                            semanticType = "";
                            bFoundTelemetry = true;
                        }
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
                            bFoundTelemetry = true;
                            semanticType = supplementalType.Versionless;
                            break;
                        }
                    }
                }

                if (bFoundTelemetry)
                {
                    // make sure payload includes data for this telemetry
                    if (signalRData.ContainsKey(telemetryInfo.Name))
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
                        data.telmetryInterfaceId = telemetryInfo.ChildOf.AbsoluteUri;
                    }
                }
            }
            else if (telemetryInfo.Schema.EntityKind == DTEntityKind.Object)
            {
                if (model_id.StartsWith("dtmi:azureiot:PhoneAsADevice"))
                {
                    // we are interested in accelerometer z to determine face up or down.
                    if (telemetryInfo.Name.Equals("accelerometer") && signalRData.ContainsKey(telemetryInfo.Name))
                    {
                        JObject accelerometerData = (JObject)signalRData[telemetryInfo.Name];

                        DTObjectInfo dtObject = telemetryInfo.Schema as DTObjectInfo;

                        foreach (var dtObjFeild in dtObject.Fields)
                        {
                            if (dtObjFeild.Id.AbsoluteUri.StartsWith("dtmi:iotcentral:schema:vector") && dtObjFeild.Name.Equals("z"))
                            {
                                if (accelerometerData.ContainsKey(dtObjFeild.Name))
                                {
                                    data = new TELEMETRY_DATA();
                                    data.dataKind = telemetryInfo.Schema.EntityKind;
                                    data.name = $"{telemetryInfo.Name}.{dtObjFeild.Name}";
                                    data.dataDouble = (double)accelerometerData[dtObjFeild.Name];
                                    data.dataName = $"{telemetryInfo.Name}.{dtObjFeild.Name}";
                                    data.semanticType = "";
                                    data.telmetryInterfaceId = telemetryInfo.ChildOf.AbsoluteUri;
                                    break;
                                }
                            }
                        }
                    }
                    //else if (telemetryInfo.Name.Equals("gyroscope") && signalRData.ContainsKey(telemetryInfo.Name))
                    //{
                    //    JObject gyroData = (JObject)signalRData[telemetryInfo.Name];

                    //    DTObjectInfo dtObject = telemetryInfo.Schema as DTObjectInfo;

                    //    foreach (var dtObjFeild in dtObject.Fields)
                    //    {
                    //        if (dtObjFeild.Id.AbsoluteUri.StartsWith("dtmi:iotcentral:schema:vector") && dtObjFeild.Name.Equals("z"))
                    //        {

                    //            if (gyroData.ContainsKey(dtObjFeild.Name))
                    //            {
                    //                _logger.LogInformation($"!!!! Found Gyro Z {gyroData[dtObjFeild.Name]}");
                    //            }
                    //        }
                    //    }
                    //}
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
//            catch (
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
            public string telmetryInterfaceId { get; set; }
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

        public enum PHONE_POSE_UP_DOWN
        {
            Down,
            Up,
            Unkonwn
        }

        public class PHONE_POSE_DATA
        {
            public string deviceId { get; set; }
            public PHONE_POSE_UP_DOWN pose { get; set; }
        }
    }

}
